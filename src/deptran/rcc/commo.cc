
#include "commo.h"
#include "dep_graph.h"
#include "../procedure.h"
#include "graph_marshaler.h"
#include "../service.h"

namespace janus {

void RccCommo::SendDispatch(vector<SimpleCommand> &cmd,
                            const function<void(int res,
                                                TxnOutput&,
                                                RccGraph&)>& callback) {
  rrr::FutureAttr fuattr;
  auto tid = cmd[0].root_id_;
  auto par_id = cmd[0].partition_id_;
  std::function<void(Future*)> cb =
      [callback, tid, par_id](Future* fu) {
        int res;
        TxnOutput output;
        MarshallDeputy md;
        fu->get_reply() >> res >> output >> md;
        if (md.kind_ == MarshallDeputy::EMPTY_GRAPH) {
          RccGraph rgraph;
          auto v = rgraph.CreateV(tid);
          RccTx& info = *v;
//          info.partition_.insert(par_id);
          verify(rgraph.vertex_index().size() > 0);
          callback(res, output, rgraph);
        } else if (md.kind_ == MarshallDeputy::RCC_GRAPH) {
          RccGraph& graph = dynamic_cast<RccGraph&>(*md.sp_data_);
          callback(res, output, graph);
        } else {
          verify(0);
        }
      };
  fuattr.callback = cb;
  auto proxy = NearestProxyForPartition(cmd[0].PartitionId()).second;
  Log_debug("dispatch to %ld", cmd[0].PartitionId());
//  verify(cmd.type_ > 0);
//  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy->async_JanusDispatch(cmd, fuattr));
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


shared_ptr<map<txid_t, parent_set_t>>
RccCommo::Inquire(parid_t pid, txnid_t tid, rank_t rank) {
  auto ret = std::make_shared<map<txid_t, parent_set_t>>();
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  FutureAttr fuattr;
  function<void(Future*)> cb = [ret, &ev] (Future* fu) {
//    MarshallDeputy md;
    fu->get_reply() >> *ret;
    ev->Set(1);
  };
  fuattr.callback = cb;
  auto proxy = (ClassicProxy*)NearestProxyForPartition(pid).second;
  Future::safe_release(proxy->async_RccInquire(tid, rank, fuattr));
//  ev->Wait(60*1000*1000);
//  verify(ev->status_ != Event::TIMEOUT);
  ev->Wait();
  return ret;
}

void RccCommo::SendInquire(parid_t pid,
                           epoch_t epoch,
                           txnid_t tid,
                           const function<void(RccGraph& graph)>& callback) {
  // verify(0);
//  FutureAttr fuattr;
//  function<void(Future*)> cb = [callback] (Future* fu) {
//    MarshallDeputy md;
//    fu->get_reply() >> md;
//    RccGraph& graph = dynamic_cast<RccGraph&>(*md.sp_data_);
//    callback(graph);
//  };
//  fuattr.callback = cb;
//  auto proxy = (ClassicProxy*)NearestProxyForPartition(pid).second;
//  Future::safe_release(proxy->async_RccInquire(tid, fuattr));
}

void RccCommo::BroadcastCommit(parid_t par_id,
                               txnid_t cmd_id,
                               rank_t rank,
                               bool need_validation,
                               shared_ptr<RccGraph> graph,
                               const function<void(int32_t, TxnOutput&)>& callback) {
  verify(0);
  bool skip_graph = IsGraphOrphan(*graph, cmd_id);
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto& p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback](Future* fu) {
                        int32_t res;
                        TxnOutput output;
                        fu->get_reply() >> res >> output;
                        callback(res, output);
                      };
    verify(cmd_id > 0);
    if (skip_graph) {
      Future::safe_release(proxy->async_JanusCommitWoGraph(cmd_id, RANK_UNDEFINED, need_validation, fuattr));
    } else {
      MarshallDeputy md(graph);
      Future::safe_release(proxy->async_JanusCommit(cmd_id, RANK_UNDEFINED, need_validation, md, fuattr));
    }
  }
}

bool RccCommo::IsGraphOrphan(RccGraph& graph, txnid_t cmd_id) {
  if (graph.size() == 1) {
    auto v = graph.FindV(cmd_id);
    verify(v);
    return true;
  } else {
    return false;
  }
}

void RccCommo::BroadcastValidation(txid_t id, set<parid_t> pars, int result) {
  for (auto partition_id : pars) {
    for (auto& pair : rpc_par_proxies_[partition_id]) {
      auto proxy = pair.second;
      FutureAttr fuattr;
      fuattr.callback = [] (Future* fu) {
      };
      int rank = RANK_D;
      verify(0);
      Future::safe_release(proxy->async_RccNotifyGlobalValidation(id, rank, result, fuattr));
    }
  }
}

} // namespace janus
