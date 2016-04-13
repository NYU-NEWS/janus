

#include "sched.h"
#include "../rcc/dtxn.h"

using namespace rococo;

void BrqSched::OnPreAccept(const txnid_t txn_id,
                           const RccGraph& graph,
                           int32_t* res,
                           RccGraph* res_graph,
                           function<void()> callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(txn_id > 0);
  dep_graph_->Aggregate(const_cast<RccGraph&>(graph));
  dep_graph_->MinItfrGraph(txn_id, res_graph);
  *res = SUCCESS;
  RccDTxn *dtxn = (RccDTxn*) GetOrCreateDTxn(txn_id);
  callback();
} //

void BrqSched::OnCommit(const txnid_t txn_id,
                        const RccGraph& graph,
                        int32_t* res,
                        TxnOutput* output,
                        function<void()> callback) {
  // TODO to support cascade abort
  *res = SUCCESS;
  RccSched::OnCommit(txn_id, graph, output, callback);
}