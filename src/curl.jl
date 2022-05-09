const StatusCode = (
    OK                  = (code=0,  message="Success"),
    CANCELLED           = (code=1,  message="The operation was cancelled"),
    UNKNOWN             = (code=2,  message="Unknown error"),
    INVALID_ARGUMENT    = (code=3,  message="Client specified an invalid argument"),
    DEADLINE_EXCEEDED   = (code=4,  message="Deadline expired before the operation could complete"),
    NOT_FOUND           = (code=5,  message="Requested entity was not found"),
    ALREADY_EXISTS      = (code=6,  message="Entity already exists"),
    PERMISSION_DENIED   = (code=7,  message="No permission to execute the specified operation"),
    RESOURCE_EXHAUSTED  = (code=8,  message="Resource exhausted"),
    FAILED_PRECONDITION = (code=9,  message="Operation was rejected because the system is not in a state required for the operation's execution"),
    ABORTED             = (code=10, message="Operation was aborted"),
    OUT_OF_RANGE        = (code=11, message="Operation was attempted past the valid range"),
    UNIMPLEMENTED       = (code=12, message="Operation is not implemented or is not supported/enabled in this service"),
    INTERNAL            = (code=13, message="Internal error"),
    UNAVAILABLE         = (code=14, message="The service is currently unavailable"),
    DATA_LOSS           = (code=15, message="Unrecoverable data loss or corruption"),
    UNAUTHENTICATED     = (code=16, message="The request does not have valid authentication credentials for the operation")
)

grpc_status_info(code) = StatusCode[code+1]
grpc_status_message(code) = (grpc_status_info(code)).message
grpc_status_code_str(code) = string(propertynames(StatusCode)[code+1])

#=
const SEND_BUFFER_SZ = 1024 * 1024
function buffer_send_data(input::Channel{T}) where T <: ProtoType
    data = nothing
    if isready(input)
        iob = IOBuffer()
        while isready(input) && (iob.size < SEND_BUFFER_SZ)
            write(iob, to_delimited_message_bytes(take!(input)))
            yield()
        end
        data = take!(iob)
    elseif isopen(input)
        data = UInt8[]
    end
    data
end
=#

function send_data(easy::Curl.Easy, input::Channel{T}, max_send_message_length::Int) where T <: ProtoType
	for input_obj in input
		input_buf = to_delimited_message_buffer( input_obj, max_send_message_length )
		Downloads.upload_data( easy, input_buf ) 
		# `upload_data` is missing the `yield()` statement
		# but if I put it there, something errors
	end
	return nothing
end

function to_delimited_message_buffer(msg, max_message_length::Int)
    iob = IOBuffer()
    limitiob = LimitIO(iob, max_message_length)
    write(limitiob, UInt8(0))                   # compression
    write(limitiob, hton(UInt32(0)))            # message length (placeholder)
    data_len = writeproto(limitiob, msg)        # message bytes

    seek(iob, 1)                                # seek out the message length placeholder
    write(iob, hton(UInt32(data_len)))          # fill the message length
    seek(iob, 0)
    return iob
end

function grpc_timeout_header_val(timeout::Real)
    if round(Int, timeout) == timeout
        timeout_secs = round(Int64, timeout)
        return "$(timeout_secs)S"
    end
    timeout *= 1000
    if round(Int, timeout) == timeout
        timeout_millisecs = round(Int64, timeout)
        return "$(timeout_millisecs)m"
    end
    timeout *= 1000
    if round(Int, timeout) == timeout
        timeout_microsecs = round(Int64, timeout)
        return "$(timeout_microsecs)u"
    end
    timeout *= 1000
    timeout_nanosecs = round(Int64, timeout)
    return "$(timeout_nanosecs)n"
end

function generate_grpc_headers_dict(; request_timeout::Real=Inf)
    headers = Dict(
        "User-Agent" =>  "$(Curl.USER_AGENT)",
        "Content-Type" =>  "application/grpc+proto", 
		"Content-Length" => "",
        "te" =>  "trailers"
    )
    if !isinf(request_timeout)
        headers["grpc-timeout"] =  "$(grpc_timeout_header_val(request_timeout))"
    end
    return headers
end

function grpc_headers_dict(; request_timeout::Real=Inf )
    if isinf(request_timeout)
        return GRPC_STATIC_HEADERS_DICT
    else
        return generate_grpc_headers_dict(; request_timeout )
    end
end

function easy_handle(;
	maxage::Clong, 
	keepalive::Clong, 
	negotiation::Symbol, 
	revocation::Bool
)
    easy = Curl.Easy()
    http_version = (negotiation === :http2) ? CURL_HTTP_VERSION_2_0 :
                   (negotiation === :http2_tls) ? CURL_HTTP_VERSION_2TLS :
                   (negotiation === :http2_prior_knowledge) ? CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE :
                   throw(ArgumentError("unsupported HTTP2 negotiation mode $negotiation"))
    Curl.setopt(easy, CURLOPT_HTTP_VERSION, http_version)
    Curl.setopt(easy, CURLOPT_PIPEWAIT, Clong(1))  
	#Curl.setopt(easy, CURLOPT_POST, Clong(1))
	#Curl.setopt(easy, CURLOPT_HTTPHEADER, grpc_request_header(request_timeout))
    if !revocation
        Curl.setopt(easy, CURLOPT_SSL_OPTIONS, CURLSSLOPT_NO_REVOKE)
    end
    if maxage > 0
        Curl.setopt(easy, CURLOPT_MAXAGE_CONN, maxage)
    end
    if keepalive > 0
        Curl.setopt(easy, CURLOPT_TCP_KEEPALIVE, Clong(1))
        Curl.setopt(easy, CURLOPT_TCP_KEEPINTVL, keepalive);
        Curl.setopt(easy, CURLOPT_TCP_KEEPIDLE, keepalive);
	end
 
	return easy
end

function recv_data(easy::Curl.Easy, output::Channel{T}, max_recv_message_length::Int) where T <: ProtoType
    iob = PipeBuffer()
    waiting_for_header = true
    msgsize = 0
    compressed = UInt8(0)
    datalen = UInt32(0)
    need_more = true
    for buf in easy.output
        write(iob, buf)
        need_more = false
        while !need_more
            if waiting_for_header
                if bytesavailable(iob) >= 5
                    compressed = read(iob, UInt8)       # compression
                    datalen = ntoh(read(iob, UInt32))   # message length

                    if datalen > max_recv_message_length
                        throw(gRPCMessageTooLargeException(max_recv_message_length, datalen))
                    end

                    waiting_for_header = false
                else
                    need_more = true
                end
            end

            if !waiting_for_header
                if bytesavailable(iob) >= datalen
                    msgbytes = IOBuffer(view(iob.data, iob.ptr:(iob.ptr+datalen-1)))
                    put!(output, readproto(msgbytes, T()))  # decode message bytes
                    iob.ptr += datalen
                    waiting_for_header = true
                else
                    need_more = true
                end
            end
        end
    end
    close(output)
end

function set_low_speed_limits(easy::Curl.Easy, low_speed_limit, low_speed_time)
    low_speed_limit >= 0 || 
        throw(ArgumentError("`low_speed_limit` must be non-negative, got $(low_speed_limit)."))
    low_speed_time >= 0 || 
        throw(ArgumentError("`low_speed_time` must be non-negative, got $(low_speed_time)."))
    
    _max = Clong(typemax(Clong))
    _low_speed_limit = low_speed_limit <= _max ? round(Clong, low_speed_limit) : _max
    _low_speed_time = low_speed_time <= _max ? round(Clong, low_speed_time) : _max
    
    Curl.setopt(easy, CURLOPT_LOW_SPEED_LIMIT, _low_speed_limit)    
    Curl.setopt(easy, CURLOPT_LOW_SPEED_TIME, _low_speed_time)
    return nothing    
end 

function set_connect_timeout(easy::Curl.Easy, timeout::Real)
    timeout >= 0 ||
        throw(ArgumentError("timeout must be positive, got $timeout"))
    if timeout ≤ typemax(Clong) ÷ 1000
        timeout_ms = round(Clong, timeout * 1000)
        Curl.setopt(easy, CURLOPT_CONNECTTIMEOUT_MS, timeout_ms)
    else
        timeout = timeout ≤ typemax(Clong) ? round(Clong, timeout) : Clong(0)
        Curl.setopt(easy, CURLOPT_CONNECTTIMEOUT, timeout)
    end
end

function grpc_request(
	downloader::Downloader, url::String, 
	input_ch::Channel{T1}, output_ch::Channel{T2};
	maxage::Clong = typemax(Clong),
	keepalive::Clong = 60,
	negotiation::Symbol = :http2_prior_knowledge,
	revocation::Bool = true,
	request_timeout::Real = Inf,
	connect_timeout::Real = 0,
	max_recv_message_length::Int = DEFAULT_MAX_RECV_MESSAGE_LENGTH,
	max_send_message_length::Int = DEFAULT_MAX_SEND_MESSAGE_LENGTH,
	verbose::Bool = false,
	low_speed_limit::Int = 0,
	low_speed_time::Int = 0
)::gRPCStatus where {T1 <: ProtoType, T2 <: ProtoType}

	Curl.with_handle(
		easy_handle(; maxage, keepalive, negotiation, revocation) 
	) do easy
		
		# -- similar to `Downloads.request`
		# and "old" `gRPCClient.grpc_request`
		Curl.set_url(easy, url)
		Curl.set_timeout(easy, request_timeout)
		Curl.set_verbose(easy, verbose)
		
		set_connect_timeout(easy, connect_timeout)
    	set_low_speed_limits(easy, low_speed_limit, low_speed_time)

		headers = grpc_headers_dict(; request_timeout)
		Curl.add_headers(easy, headers)
		
		Curl.enable_upload(easy)
		method = "POST"
		Curl.set_method(easy, "POST")

		Downloads.set_ca_roots(downloader, easy)
		
		info = (url = url, method = method, headers = headers)
		Downloads.easy_hook(downloader, easy, info)

		# do the request
		Curl.add_handle(downloader.multi, easy)

		try # ensure handle is removed
			Base.Experimental.@sync begin
				@async recv_data(easy, output_ch, max_recv_message_length)
				
				@async send_data(easy, input_ch, max_send_message_length)
			end
		finally
			Curl.remove_handle(downloader.multi, easy)
			close(easy.output)
			close(easy.progress)
			close(input_ch)
			close(output_ch)
		end

		# parse the grpc headers
		response = Downloads.Response(Curl.get_response_info(easy)...)
		grpc_status = StatusCode.OK.code
        grpc_message = ""
        for (hdr_key, hdr_val) in response.headers
            if startswith(hdr_key, "grpc-status")
                grpc_status = parse(Int, strip(hdr_val))
            elseif startswith(hdr_key, "grpc-message")
                grpc_message = string(strip(hdr_val))
            end
        end
        if (easy.code == CURLE_OPERATION_TIMEDOUT) && (grpc_status == StatusCode.OK.code)
            grpc_status = StatusCode.DEADLINE_EXCEEDED.code
        end
        if (grpc_status != StatusCode.OK.code) && isempty(grpc_message)
            grpc_message = grpc_status_message(grpc_status)
        end

        if ((easy.code == CURLE_OK) && (grpc_status == StatusCode.OK.code))
            return gRPCStatus(true, grpc_status, "")
        else
            return gRPCStatus(false, grpc_status, isempty(grpc_message) ? Curl.get_curl_errstr(easy) : grpc_message)
        end

	end#with_handle
end