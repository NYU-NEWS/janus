#include "all.h"
#include "scheduler.h"

namespace rococo {

TPLDTxn::TPLDTxn(i64 tid, Scheduler *mgr) : DTxn(tid, mgr) {
  verify(mdb_txn_ == nullptr);
  mdb_txn_ = mgr->GetOrCreateMTxn(tid_);
  verify(mdb_txn_ != nullptr);
}

bool TPLDTxn::ReadColumn(
    mdb::Row *row,
    mdb::column_id_t col_id,
    Value *value) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->read_column(row, col_id, value);
};

//int TPLDTxn::start_launch(
//    const RequestHeader &header,
//    const std::vector<mdb::Value> &input,
//    const rrr::i32 &output_size,
//    rrr::i32 *res,
//    std::vector<mdb::Value> *output,
//    rrr::DeferredReply *defer) {
//  if (IS_MODE_2PL) {
//    verify(this->mdb_txn_->rtti() == mdb::symbol_t::TXN_2PL);
//    DragonBall *defer_reply_db = new DragonBall(1, [defer, res]() {
//      defer->reply();
//    });
//    this->pre_execute_2pl(header, input, res, output, defer_reply_db);
//
//  } else if (IS_MODE_NONE) {
//    this->execute(header, input, res, output);
//    defer->reply();
//
//  } else if (IS_MODE_OCC) {
//    this->execute(header, input, res, output);
//    defer->reply();
//  } else {
//    verify(0);
//  }
//  return 0;
//}


} // namespace rococo
