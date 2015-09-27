#include "commo.h"
#include "marshal-value.h"

namespace rococo {

Commo::Commo(std::vector<std::string> &addrs) {
  rpc_poll_ = new PollMgr(1);
  for (auto &addr : addrs) {
    rrr::Client *rpc_cli = new rrr::Client(rpc_poll_);
    Log::info("connect to site: %s", addr.c_str());
    auto ret = rpc_cli->connect(addr.c_str());
    verify(ret == 0);
    RococoProxy *rpc_proxy = new RococoProxy(rpc_cli);
    vec_rpc_cli_.push_back(rpc_cli);
    vec_rpc_proxy_.push_back(rpc_proxy);
  }

//  if (Config::get_config()->do_logging()) {
//    std::string log_path(Config::get_config()->log_path());
//    log_path.append(std::to_string(coo_id_));
//    recorder_ = new Recorder(log_path.c_str());
//    rpc_poll_->add(recorder_);
//  }
}

void Commo::SendStart(groupid_t gid, RequestHeader &header, std::vector<Value> &input, int32_t output_size,
                      std::function<void(Future *fu)> &callback) {
  rrr::FutureAttr fuattr;
  fuattr.callback = callback;
  RococoProxy *proxy = vec_rpc_proxy_[gid];
  Future::safe_release(proxy->async_start_pie(header, input, output_size, fuattr));
}

void Commo::SendPrepare(groupid_t gid, txnid_t tid, std::vector<int32_t> &sids, std::function<void(Future *fu)> &callback) {
  FutureAttr fuattr;
  fuattr.callback = callback;
  RococoProxy *proxy = vec_rpc_proxy_[gid];
  Future::safe_release(proxy->async_prepare_txn(tid, sids, fuattr));
}

} // namespace
