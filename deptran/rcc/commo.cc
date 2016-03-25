
#pragma once

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
        Marshal &m = fu->get_reply();
        RccGraph graph;
        m >> res >> cmd.output >> graph;
        callback(res, cmd, graph);
      };
  fuattr.callback = cb;
  auto proxy = (RococoProxy*)RandomProxyForPartition(cmd.PartitionId()).second;
  Log_debug("dispatch to %ld", cmd.PartitionId());
  verify(cmd.type_ > 0);
  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy->async_Handout(cmd, fuattr));
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
                          const function<void(int res,
                                              map<int, map<int32_t,
                                                           Value>>& output)> &callback) {
  FutureAttr fuattr;
  function<void(Future*)> cb = [] (Future* fu) {
    // verify(0);
  };
  fuattr.callback = cb;
  auto proxy = (RococoProxy*)RandomProxyForPartition(pid).second;
  Future::safe_release(proxy->async_Finish(tid, graph, fuattr));
}

void RccCommo::SendInquire(parid_t pid,
                           txnid_t tid,
                           const function<void(RccGraph& graph)>& callback) {
  verify(0);
}


} // namespace rococo