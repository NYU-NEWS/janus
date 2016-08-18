#include "commo.h"
#include "marshal-value.h"
#include "coordinator.h"

namespace rococo {

void RococoCommunicator::SendDispatch(vector<SimpleCommand>& cmd,
                                      Coordinator *coo,
                                      const function<void(int,
                                                          TxnOutput&)>
                                      &callback) {
  rrr::FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [coo, this, callback] (Future *fu) {
        int res;
        TxnOutput outputs;
        fu->get_reply() >> res >> outputs;
        callback(res, outputs);
  };
  fuattr.callback = cb;
  auto proxy = LeaderProxyForPartition(cmd[0].PartitionId());
  Log_debug("SendStart to %ld from %ld", proxy.first, coo->coo_id_);
//  verify(cmd.type_ > 0);
//  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy.second->async_Dispatch(cmd, fuattr));
}

void RococoCommunicator::SendStart(SimpleCommand &cmd,
                                   int32_t output_size,
                                   std::function<void(Future *fu)> &callback) {
  verify(0);
}

void RococoCommunicator::SendPrepare(groupid_t gid,
                                     txnid_t tid,
                                     std::vector<int32_t> &sids,
                                     const function<void(int)> &callback) {
  FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [this, callback] (Future *fu) {
        int res;
        fu->get_reply() >> res;
        callback(res);
      };
  fuattr.callback = cb;
  ClassicProxy *proxy = LeaderProxyForPartition(gid).second;
  Log_debug("SendPrepare to %ld sites gid:%ld, tid:%ld\n", sids.size(), gid, tid);
  Future::safe_release(proxy->async_Prepare(tid, sids, fuattr));
}

void RococoCommunicator::___LogSent(parid_t pid, txnid_t tid) {
  auto value = std::make_pair(pid, tid);
  auto it = phase_three_sent_.find(value);
  if (it != phase_three_sent_.end()) {
    Log_fatal("phase 3 sent exists: %d %x", it->first, it->second);
  } else {
    phase_three_sent_.insert(value);
    Log_debug("phase 3 sent: pid: %d; tid: %x", value.first, value.second);
  }
}

void RococoCommunicator::SendCommit(parid_t pid,
                                    txnid_t tid,
                                    const function<void()> &callback) {
#ifdef LOG_LEVEL_AS_DEBUG
  ___LogSent(pid, tid);
#endif
  FutureAttr fuattr;
  fuattr.callback = [callback] (Future*) {callback();};
  ClassicProxy *proxy = LeaderProxyForPartition(pid).second;
  Log_debug("SendCommit to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_Commit(tid, fuattr));
}

void RococoCommunicator::SendAbort(parid_t pid, txnid_t tid,
                                   const function<void()> &callback) {
#ifdef LOG_LEVEL_AS_DEBUG
  ___LogSent(pid, tid);
#endif
  FutureAttr fuattr;
  fuattr.callback = [callback] (Future*) {callback();};
  ClassicProxy *proxy = LeaderProxyForPartition(pid).second;
  Log_debug("SendAbort to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_Abort(tid, fuattr));
}

void RococoCommunicator::SendUpgradeEpoch(epoch_t curr_epoch,
                                const function<void(parid_t,
                                                    siteid_t,
                                                    int32_t&)>& callback) {
  for (auto& pair: rpc_par_proxies_) {
    auto& par_id = pair.first;
    auto& proxies = pair.second;
    for (auto& pair: proxies) {
      FutureAttr fuattr;
      auto& site_id = pair.first;
      function<void(Future*)> cb = [callback, par_id, site_id] (Future* fu) {
        int32_t res;
        fu->get_reply() >> res;
        callback(par_id, site_id, res);
      };
      fuattr.callback = cb;
      auto proxy = (ClassicProxy*) pair.second;
      Future::safe_release(proxy->async_UpgradeEpoch(curr_epoch, fuattr));
    }
  }
}

void RococoCommunicator::SendTruncateEpoch(epoch_t old_epoch) {
  for (auto& pair: rpc_par_proxies_) {
    auto& par_id = pair.first;
    auto& proxies = pair.second;
    for (auto& pair: proxies) {
      FutureAttr fuattr;
      fuattr.callback = [] (Future*) {};
      auto proxy = (ClassicProxy*) pair.second;
      Future::safe_release(proxy->async_TruncateEpoch(old_epoch));
    }
  }
}

} // namespace
