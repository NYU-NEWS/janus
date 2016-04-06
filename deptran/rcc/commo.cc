
#include "commo.h"
#include "dep_graph.h"
#include "txn-info.h"
#include "graph_marshaler.h"
#include "rcc_srpc.h"

namespace rococo {

void RccCommo::SendHandout(SimpleCommand &cmd,
                           const function<void(int res,
                                               SimpleCommand& cmd,
                                               RccGraph& graph)>& callback) {
  rrr::FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [callback, &cmd] (Future *fu) {
        int res;
        RccGraph graph;
        fu->get_reply() >> res >> cmd.output >> graph;
        callback(res, cmd, graph);
      };
  fuattr.callback = cb;
  auto proxy = (RococoProxy*)LeaderProxyForPartition(cmd.PartitionId()).second;
  Log_debug("dispatch to %ld", cmd.PartitionId());
  verify(cmd.type_ > 0);
  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy->async_Dispatch(cmd, fuattr));
}

void RccCommo::SendHandoutRo(SimpleCommand &cmd,
                             const function<void(int res,
                                                 SimpleCommand& cmd,
                                                 map<int, mdb::version_t>& vers)>&) {
  verify(0);
}

void RccCommo::SendFinish(parid_t pid,
                          txnid_t tid,
                          RccGraph& graph,
                          const function<void(TxnOutput& output)> &callback) {
  FutureAttr fuattr;
  function<void(Future*)> cb = [callback] (Future* fu) {
    map<innid_t, map<int32_t, Value>> outputs;
    fu->get_reply() >> outputs;
    callback(outputs);
  };
  fuattr.callback = cb;
  auto proxy = (RococoProxy*)LeaderProxyForPartition(pid).second;
  Future::safe_release(proxy->async_Finish(tid, graph, fuattr));
}

void RccCommo::SendInquire(parid_t pid,
                           txnid_t tid,
                           const function<void(RccGraph& graph)>& callback) {
  FutureAttr fuattr;
  function<void(Future*)> cb = [callback] (Future* fu) {
    RccGraph graph;
    fu->get_reply() >> graph;
    callback(graph);
  };
  fuattr.callback = cb;
  auto proxy = (RococoProxy*)LeaderProxyForPartition(pid).second;
  Future::safe_release(proxy->async_Inquire(tid, fuattr));
}


} // namespace rococo