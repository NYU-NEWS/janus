#include <deptran/txn_reg.h>
#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "deptran/procedure.h"
#include "../rcc/graph_marshaler.h"
#include "sched.h"
#include "../tpl/exec.h"

namespace rococo {

// TODO this should be a universal function.
int NoneSched::OnDispatch(const vector<TxnPieceData> &v_data,
                          rrr::i32 *res,
                          TxnOutput *output,
                          const function<void()>& callback) {
  DTxn& dtxn = *GetOrCreateDTxn(v_data[0].root_id_);
  verify(partition_id_ == v_data[0].partition_id_);
  for (auto& d : v_data) {
    TxnPieceDef& p = txn_reg_->get(d.root_type_, d.type_);
    vector<conf_id_t>& conflicts = p.conflicts_;
    HandleConflicts(dtxn.tid_, conflicts);
    p.proc_handler_(nullptr,
                    &dtxn,
                    const_cast<TxnPieceData&>(d),
                    res,
                    (*output)[d.inn_id()]);

  }

  *res = SUCCESS;
  callback();
  return 0;
}

void NoneSched::HandleConflicts(txnid_t txn_id,
                                vector<conf_id_t>& conflicts) {
  // do nothing
}

} // namespace rococo