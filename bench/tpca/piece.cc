
#include "deptran/__dep__.h"
#include "piece.h"

namespace rococo {

char TPCA_BRANCH[] = "branch";
char TPCA_TELLER[] = "teller";
char TPCA_CUSTOMER[] = "customer";

void TpcaPiece::reg_all() {
    reg_pieces();
}

void TpcaPiece::reg_pieces() {
  INPUT_PIE(TPCA_PAYMENT, TPCA_PAYMENT_1, TPCA_VAR_X)
  SHARD_PIE(TPCA_PAYMENT, TPCA_PAYMENT_1, TPCA_CUSTOMER, TPCA_VAR_X)
  CONFLICT_PIE(TPCA_PAYMENT, TPCA_PAYMENT_1,
               conf_id(TPCA_CUSTOMER, {TPCA_VAR_X}, {1}, TPCA_ROW_1));
  BEGIN_PIE(TPCA_PAYMENT, TPCA_PAYMENT_1, DF_REAL) {
//        Log::debug("output: %p, output_size: %p", output, output_size);
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
    mdb::Txn *txn = dtxn->mdb_txn_;
    Value buf;
    verify(cmd.input.size() >= 1);

    mdb::Row *r = NULL;
    mdb::MultiBlob mb(1);
    mb[0] = cmd.input.at(TPCA_VAR_X).get_blob();

    r = dtxn->Query(dtxn->GetTable(TPCA_CUSTOMER), mb, TPCA_ROW_1);
    dtxn->ReadColumn(r, 1, &buf, TXN_BYPASS);
    buf.set_i32(buf.get_i32() + 1/*input[1].get_i32()*/);
    dtxn->WriteColumn(r, 1, buf, TXN_DEFERRED);

    output[TPCA_VAR_X] = buf;
    *res = SUCCESS;
  } END_PIE

  INPUT_PIE(TPCA_PAYMENT, TPCA_PAYMENT_2, TPCA_VAR_Y)
  SHARD_PIE(TPCA_PAYMENT, TPCA_PAYMENT_2, TPCA_TELLER, TPCA_VAR_Y)
  CONFLICT_PIE(TPCA_PAYMENT, TPCA_PAYMENT_2,
               conf_id(TPCA_CUSTOMER, {TPCA_VAR_Y}, {1}, TPCA_ROW_2));
  BEGIN_PIE(TPCA_PAYMENT, TPCA_PAYMENT_2, DF_REAL) {
    mdb::Txn *txn = dtxn->mdb_txn_;
    Value buf;
    verify(cmd.input.size() >= 1);
    mdb::Row *r = NULL;
    mdb::MultiBlob mb(1);
    mb[0] = cmd.input.at(TPCA_VAR_Y).get_blob();

    r = dtxn->Query(dtxn->GetTable(TPCA_TELLER), mb, TPCA_ROW_2);
    dtxn->ReadColumn(r, 1, &buf, TXN_BYPASS);
    buf.set_i32(buf.get_i32() + 1/*input[1].get_i32()*/);
    dtxn->WriteColumn(r, 1, buf, TXN_DEFERRED);
    *res = SUCCESS;
  } END_PIE

  INPUT_PIE(TPCA_PAYMENT, TPCA_PAYMENT_3, TPCA_VAR_Z)
  SHARD_PIE(TPCA_PAYMENT, TPCA_PAYMENT_3, TPCA_BRANCH, TPCA_VAR_Z)
  CONFLICT_PIE(TPCA_PAYMENT, TPCA_PAYMENT_3,
               conf_id(TPCA_CUSTOMER, {TPCA_VAR_Z}, {1}, TPCA_ROW_3));
  BEGIN_PIE(TPCA_PAYMENT, TPCA_PAYMENT_3, DF_REAL) {
    mdb::Txn *txn = dtxn->mdb_txn_;
    Value buf;
    verify(cmd.input.size() >= 1);
    i32 output_index = 0;

    mdb::Row *r = NULL;
    mdb::MultiBlob mb(1);
    mb[0] = cmd.input.at(TPCA_VAR_Z).get_blob();

    r = dtxn->Query(dtxn->GetTable(TPCA_BRANCH), mb, TPCA_ROW_3);
    dtxn->ReadColumn(r, 1, &buf, TXN_BYPASS);
    buf.set_i32(buf.get_i32() + 1/*input[1].get_i32()*/);
    dtxn->WriteColumn(r, 1, buf, TXN_DEFERRED);

    *res = SUCCESS;
  } END_PIE
}

} // namespace rococo

