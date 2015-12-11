#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "exec.h"
#include "sched.h"

namespace rococo {

int OCCExecutor::start_launch(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    const rrr::i32 &output_size,
    rrr::i32 *res,
    std::vector<mdb::Value> *output,
    rrr::DeferredReply *defer) {

  this->execute(header, input, res, output);
  defer->reply();

  return 0;
}


int OCCExecutor::prepare() {
  verify(Config::config_s->mode_ == MODE_OCC);
  auto txn = (mdb::TxnOCC *)mdb_txn_;
  verify(txn != NULL);

  if (txn->commit_prepare())
    return SUCCESS;
  else
    return REJECT;
}


int OCCExecutor::commit() {
  verify(mdb_txn_ != NULL);
//  verify(mdb_txn_ == sched_->del_mdb_txn(cmd_id_));
  ((mdb::TxnOCC *) mdb_txn_)->commit_confirm();
  delete mdb_txn_;
  return SUCCESS;
}


} // namespace rococo