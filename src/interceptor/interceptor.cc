// src/interceptor/interceptor.cc
#include "interceptor/interceptor.h"
#include "protocol/rpc_service.pb.h"

namespace rpc {

RpcResponse InterceptorChain::execute(RpcRequest& req, Handler handler) {
    // preHandle 顺序执行
    for (size_t i = 0; i < interceptors_.size(); ++i) {
        if (!interceptors_[i]->preHandle(req)) {
            RpcResponse errorResp;
            errorResp.set_success(false);
            errorResp.set_error_msg("interceptor blocked");
            return errorResp;
        }
    }

    // 执行实际处理
    RpcResponse resp = handler(req);

    // postHandle 逆序执行
    for (size_t i = interceptors_.size(); i > 0; --i) {
        interceptors_[i - 1]->postHandle(resp);
    }

    return resp;
}

} // namespace rpc
