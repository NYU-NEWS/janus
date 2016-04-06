#include "commo.h"
#include "marshal-value.h"
#include "coordinator.h"

namespace rococo {

void RococoCommunicator::SendDispatch(SimpleCommand &cmd,
                                     Coordinator *coo,
                                     const function<void(int,
                                                         ContainerCommand&)>
                                      &callback) {
  rrr::FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [coo, this, callback, &cmd] (Future *fu) {
        Log_debug("SendStart callback for %ld from %ld",
              cmd.PartitionId(),
              coo->coo_id_);

        int res;
        Marshal &m = fu->get_reply();
        m >> res >> cmd.output;
        callback(res, cmd);
  };
  fuattr.callback = cb;
  auto proxy = LeaderProxyForPartition(cmd.PartitionId());
  Log_debug("SendStart to %ld from %ld", proxy.first, coo->coo_id_);
  verify(cmd.type_ > 0);
  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy.second->async_Dispatch(cmd, fuattr));
}

void RococoCommunicator::SendStart(SimpleCommand &cmd,
                                   int32_t output_size,
                                   std::function<void(Future *fu)> &callback) {
  rrr::FutureAttr fuattr;
  fuattr.callback = callback;
  auto proxy = LeaderProxyForPartition(cmd.PartitionId());
  Log_debug("SendStart to %ld\n", proxy.first);
  Future::safe_release(proxy.second->async_Dispatch(cmd, fuattr));
}

void RococoCommunicator::SendPrepare(groupid_t gid,
                                     txnid_t tid,
                                     std::vector<int32_t> &sids,
                                     const function<void(Future*)> &callback) {
  FutureAttr fuattr;
  fuattr.callback = callback;
  ClassicProxy *proxy = LeaderProxyForPartition(gid).second;
  Log_debug("SendPrepare to %ld sites gid:%ld, tid:%ld\n", sids.size(), gid, tid);
  Future::safe_release(proxy->async_Prepare(tid, sids, fuattr));
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
  ClassicProxy *proxy = LeaderProxyForPartition(pid).second;
  Log_debug("SendCommit to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_Commit(tid, fuattr));
}

void RococoCommunicator::SendAbort(parid_t pid, txnid_t tid,
                                   const function<void(Future *fu)> &callback) {
//  ___LogSent(pid, tid);
  FutureAttr fuattr;
  fuattr.callback = callback;
  ClassicProxy *proxy = LeaderProxyForPartition(pid).second;
  Log_debug("SendAbort to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_Abort(tid, fuattr));
}


} // namespace
