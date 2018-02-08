include (Async_kernel
         : (module type of struct include Async_kernel end
             with module Require_explicit_time_source := Async_kernel.Require_explicit_time_source))
include Async_unix

module Kernel_scheduler = Async_kernel_scheduler

module Rpc_kernel    = Async_rpc_kernel.Rpc
module Versioned_rpc = Async_rpc_kernel.Versioned_rpc

include Core.Polymorphic_compare
