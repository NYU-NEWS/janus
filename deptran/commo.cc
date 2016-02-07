#include "commo.h"
#include "marshal-value.h"
#include "coordinator.h"

namespace rococo {

void RococoCommunicator::SendStart(SimpleCommand &cmd,
                                   Coordinator *coo,
                                   const function<void(StartReply&)> &callback) {
  rrr::FutureAttr fuattr;
  RococoProxy *proxy = rpc_proxies_[cmd.GetSiteId()];
  std::function<void(Future*)> cb =
      [coo, this, callback, &cmd] (Future *fu) {
    StartReply reply;
    Log_debug("SendStart callback for %ld from %ld",
              cmd.GetSiteId(),
              coo->coo_id_);
    reply.cmd = &cmd;
    Marshal &m = fu->get_reply();
    m >> reply.res >> cmd.output;
    callback(reply);
  };
  fuattr.callback = cb;
  Log_debug("SendStart to %ld from %ld", cmd.GetSiteId(), coo->coo_id_);
  verify(cmd.type_ > 0);
  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy->async_StartTxn(cmd, fuattr));
}

void RococoCommunicator::SendStart(SimpleCommand &cmd,
                                   int32_t output_size,
                                   std::function<void(Future *fu)> &callback) {
  rrr::FutureAttr fuattr;
  fuattr.callback = callback;
  RococoProxy *proxy = rpc_proxies_[cmd.GetSiteId()];
  Log_debug("SendStart to %ld\n", cmd.GetSiteId());
  Future::safe_release(proxy->async_StartTxn(cmd, fuattr));
}

void RococoCommunicator::SendPrepare(groupid_t gid,
                                     txnid_t tid,
                                     std::vector<int32_t> &sids,
                                     const function<void(Future*)> &callback) {
  FutureAttr fuattr;
  fuattr.callback = callback;
  RococoProxy *proxy = rpc_proxies_[gid];

  Log_debug("SendPrepare to %ld sites gid:%ld, tid:%ld\n", sids.size(), gid, tid);
  Future::safe_release(proxy->async_prepare_txn(tid, sids, fuattr));
}

void RococoCommunicator::___LogSent(parid_t pid, txnid_t tid) {
  auto it = phase_three_sent_.find(std::make_pair(pid, tid));
  verify(it == phase_three_sent_.end());
  phase_three_sent_.insert(std::make_pair(pid, tid));
}

void RococoCommunicator::SendCommit(parid_t pid,
                                    txnid_t tid,
                                    const function<void(Future *fu)> &callback) {
  ___LogSent(pid, tid);

  FutureAttr fuattr;
  fuattr.callback = callback;
  RococoProxy *proxy = rpc_proxies_[pid];
  Log_debug("SendCommit to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_commit_txn(tid, fuattr));
}

void RococoCommunicator::SendAbort(parid_t pid, txnid_t tid,
                                   const function<void(Future *fu)> &callback) {
//  ___LogSent(pid, tid);
  FutureAttr fuattr;
  fuattr.callback = callback;
  RococoProxy *proxy = rpc_proxies_[pid];
  Log_debug("SendAbort to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_abort_txn(tid, fuattr));
}


} // namespace
