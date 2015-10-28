#include "commo.h"
#include "marshal-value.h"
#include "coordinator.h"

namespace rococo {

Commo::Commo(std::vector<std::string> &addrs) {
  verify(addrs.size() > 0);
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
void Commo::SendStart(parid_t par_id, 
                      StartRequest &req, 
                      Coordinator *coo,
                      std::function<void(StartReply&)> &callback) {
  rrr::FutureAttr fuattr;
  RequestHeader header;
  header.cid = coo->coo_id_;
  header.tid = req.cmd_id;
  header.pid = req.pie_id;
  header.t_type = req.cmd->GetRootCmd()->type();
  header.p_type = req.cmd->type(); // TODO
  verify(header.t_type);
  verify(header.p_type);
  SimpleCommand *cmd  = (SimpleCommand*)req.cmd;
  RococoProxy *proxy = vec_rpc_proxy_[par_id];
  std::function<void(Future*)> cb = [coo, this, callback, cmd](Future *fu) {
    StartReply reply; 
    reply.cmd = cmd;
    Marshal &m = fu->get_reply();
    m >> reply.res >> cmd->output;
    callback(reply);
  };
  fuattr.callback = cb;
  Future::safe_release(proxy->async_start_pie(header, cmd->input,
                                              cmd->output_size, fuattr));
}

void Commo::SendStart(parid_t par_id, 
                      RequestHeader &header, 
                      std::vector<Value> &input, 
                      int32_t output_size,
                      std::function<void(Future *fu)> &callback) {
  rrr::FutureAttr fuattr;
  fuattr.callback = callback;
  RococoProxy *proxy = vec_rpc_proxy_[par_id];
  Future::safe_release(proxy->async_start_pie(header, input, output_size, fuattr));
}

void Commo::SendPrepare(groupid_t gid, txnid_t tid, 
                        std::vector<int32_t> &sids, 
                        std::function<void(Future *fu)> &callback) {
  FutureAttr fuattr;
  fuattr.callback = callback;
  RococoProxy *proxy = vec_rpc_proxy_[gid];
  Future::safe_release(proxy->async_prepare_txn(tid, sids, fuattr));
}

} // namespace
