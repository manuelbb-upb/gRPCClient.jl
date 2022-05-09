module gRPCClient

using LibCURL
using Downloads
using ProtoBuf

import Downloads: Curl
import ProtoBuf: call_method

export gRPCController, gRPCChannel, gRPCException, gRPCServiceCallException, gRPCMessageTooLargeException, gRPCStatus, gRPCCheck, StatusCode

abstract type gRPCException <: Exception end

include("limitio.jl")
include("curl.jl")
include("grpc.jl")
include("generate.jl")

const GRPC_STATIC_HEADERS_DICT = generate_grpc_headers_dict()

end # module
