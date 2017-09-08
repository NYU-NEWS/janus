
#include "commo.h"
#include "dep_graph.h"
#include "deptran/procedure.h"
#include "txn-info.h"
#include "graph_marshaler.h"
#include "deptran/service.h"

namespace rococo {

void RccCommo::SendDispatch(vector<SimpleCommand> &cmd,
                            const function<void(int res,
                                                TxnOutput&,
                                                RccGraph&)>& callback) {
  rrr::FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [callback] (Future *fu) {
        int res;
        TxnOutput output;
        MarshallDeputy md;
        fu->get_reply() >> res >> output >> md;
        RccGraph& graph = dynamic_cast<RccGraph&>(*md.sp_data_);
        callback(res, output, graph);
      };
  fuattr.callback = cb;
  // TODO fix.
  auto proxy = (ClassicProxy*)NearestProxyForPartition(
      cmd[0].PartitionId()).second;
  Log_debug("dispatch to %ld", cmd[0].PartitionId());
//  verify(cmd.type_ > 0);
//  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy->async_RccDispatch(cmd, fuattr));
}

void RccCommo::SendHandoutRo(SimpleCommand &cmd,
                             const function<void(int res,
                                                 SimpleCommand& cmd,
                                                 map<int, mdb::version_t>& vers)>&) {
  verify(0);
}

void RccCommo::SendFinish(parid_t pid,
                          txnid_t tid,
                          shared_ptr<RccGraph> graph,
                          const function<void(TxnOutput& output)> &callback) {
  FutureAttr fuattr;
  function<void(Future*)> cb = [callback] (Future* fu) {
    map<innid_t, map<int32_t, Value>> outputs;
    fu->get_reply() >> outputs;
    callback(outputs);
  };
  fuattr.callback = cb;
  auto proxy = NearestProxyForPartition(pid).second;
  MarshallDeputy md(graph);
  Future::safe_release(proxy->async_RccFinish(tid, md, fuattr));
}

void RccCommo::SendInquire(parid_t pid,
                           epoch_t epoch,
                           txnid_t tid,
                           const function<void(RccGraph& graph)>& callback) {
  FutureAttr fuattr;
  function<void(Future*)> cb = [callback] (Future* fu) {
    MarshallDeputy md;
    fu->get_reply() >> md;
    RccGraph& graph = dynamic_cast<RccGraph&>(*md.sp_data_);
    callback(graph);
  };
  fuattr.callback = cb;
  auto proxy = (ClassicProxy*)NearestProxyForPartition(pid).second;
  Future::safe_release(proxy->async_RccInquire(epoch, tid, fuattr));
}


} // namespace rococo
