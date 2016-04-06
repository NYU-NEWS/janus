#include "../rcc/txn-info.h"
#include "../rcc/graph_marshaler.h"
#include "dep_graph.h"
#include "commo.h"
#include "brq_srpc.h"

namespace rococo {

void BrqCommo::SendHandout(SimpleCommand &cmd,
                           const function<void(int res,
                                               SimpleCommand& cmd,
                                               RccGraph& graph)>& callback) {
  rrr::FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [callback, &cmd] (Future *fu) {
        int res;
        BrqGraph graph;
        fu->get_reply() >> res >> cmd.output >> graph;
        callback(res, cmd, graph);
      };
  fuattr.callback = cb;
  auto proxy = (BrqProxy*)RandomProxyForPartition(cmd.PartitionId()).second;
  Log_debug("dispatch to %ld", cmd.PartitionId());
  verify(cmd.type_ > 0);
  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy->async_Dispatch(cmd, fuattr));
}

void BrqCommo::SendHandoutRo(SimpleCommand &cmd,
                             const function<void(int res,
                                                 SimpleCommand& cmd,
                                                 map<int, mdb::version_t>& vers)>&) {
  verify(0);
}

void BrqCommo::SendFinish(parid_t pid,
                          txnid_t tid,
                          RccGraph& graph,
                          const function<void(TxnOutput& output)> &callback) {
  verify(0);
  FutureAttr fuattr;
  function<void(Future*)> cb = [callback] (Future* fu) {
    int32_t res;
    TxnOutput outputs;
    fu->get_reply() >> res >> outputs;
    callback(outputs);
  };
  fuattr.callback = cb;
  auto proxy = (BrqProxy*)RandomProxyForPartition(pid).second;
  Future::safe_release(proxy->async_Commit(tid, (BrqGraph&)graph, fuattr));
}

void BrqCommo::SendInquire(parid_t pid,
                           txnid_t tid,
                           const function<void(RccGraph& graph)>& callback) {
  FutureAttr fuattr;
  function<void(Future*)> cb = [callback] (Future* fu) {
    BrqGraph graph;
    fu->get_reply() >> graph;
    callback(graph);
  };
  fuattr.callback = cb;
  auto proxy = (BrqProxy*)RandomProxyForPartition(pid).second;
  Future::safe_release(proxy->async_Inquire(tid, fuattr));
}

void BrqCommo::BroadcastPreAccept(parid_t par_id,
                                  txnid_t cmd_id,
                                  ballot_t ballot,
                                  RccGraph& graph,
                                  const function<void(int, RccGraph&)> &callback) {
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto &p : rpc_par_proxies_[par_id]) {
    auto proxy = (BrqProxy*)(p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback] (Future* fu) {
      int32_t res;
      RccGraph graph;
      fu->get_reply() >> res >> graph;
      callback(res, graph);
    };
    verify(cmd_id > 0);
    Future::safe_release(proxy->async_PreAccept(cmd_id, graph, fuattr));
  }
}

void BrqCommo::BroadcastCommit(parid_t par_id,
                               txnid_t cmd_id,
                               RccGraph& graph,
                               const function<void(int32_t, TxnOutput&)>
                               &callback) {
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto &p : rpc_par_proxies_[par_id]) {
    auto proxy = (BrqProxy*)(p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback] (Future* fu) {
      int32_t res;
      TxnOutput output;
      fu->get_reply() >> res >> output;
      callback(res, output);
    };
    verify(cmd_id > 0);
    Future::safe_release(proxy->async_Commit(cmd_id, graph, fuattr));
  }
}

} // namespace rococo