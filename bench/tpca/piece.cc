#include "all.h"
#include "tpl/tpl.h"

namespace rococo {

char TPCA_BRANCH[] = "branch";
char TPCA_TELLER[] = "teller";
char TPCA_CUSTOMER[] = "customer";

void TpcaPiece::reg_all() {
    reg_pieces();
    reg_lock_oracles();
}

void TpcaPiece::reg_pieces() {
  BEGIN_PIE(TPCA_PAYMENT, TPCA_PAYMENT_1, DF_REAL) {
//        Log::debug("output: %p, output_size: %p", output, output_size);
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
    mdb::Txn *txn = dtxn->mdb_txn_;
    Value buf;
    verify(input.size() == 1);
    i32 output_index = 0;

    mdb::Row *r = NULL;
    mdb::MultiBlob mb(1);
    mb[0] = input.at(0).get_blob();

    r = dtxn->Query(txn->get_table(TPCA_CUSTOMER), mb);
    dtxn->ReadColumn(r, 1, &buf, TXN_BYPASS);
    buf.set_i64(buf.get_i64() + 1/*input[1].get_i64()*/);
    dtxn->WriteColumn(r, 1, buf, TXN_SAFE, TXN_DEFERRED);

    output[output_index++] = buf;
    *res = SUCCESS;
  } END_PIE

    BEGIN_PIE(TPCA_PAYMENT, TPCA_PAYMENT_2, DF_REAL) {
//        Log::debug("output: %p, output_size: %p", output, output_size);
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
        mdb::Txn *txn = dtxn->mdb_txn_;
        Value buf;
        verify(input.size() == 1);
        i32 output_index = 0;

        mdb::Row *r = NULL;
        mdb::MultiBlob mb(1);
        //cell_locator_t cl(TPCA_CUSTOMER, 1);
        mb[0] = input.at(0).get_blob();

        r = dtxn->Query(txn->get_table(TPCA_TELLER), mb);


        dtxn->ReadColumn(r, 1, &buf, TXN_BYPASS);
        buf.set_i64(buf.get_i64() + 1/*input[1].get_i64()*/);
        dtxn->WriteColumn(r, 1, buf, TXN_SAFE, TXN_DEFERRED);

        *res = SUCCESS;
    } END_PIE

    BEGIN_PIE(TPCA_PAYMENT, TPCA_PAYMENT_3, DF_REAL) {
//        Log::debug("output: %p, output_size: %p", output, output_size);
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                                                       mdb::Txn *txn = dtxn->mdb_txn_;
        Value buf;
        verify(input.size() == 1);
        i32 output_index = 0;

        mdb::Row *r = NULL;
        mdb::MultiBlob mb(1);
        //cell_locator_t cl(TPCA_BRANCH, 1);
        mb[0] = input.at(0).get_blob();

        r = dtxn->Query(txn->get_table(TPCA_BRANCH), mb);

        dtxn->ReadColumn(r, 1, &buf, TXN_BYPASS);
        buf.set_i64(buf.get_i64() + 1/*input[1].get_i64()*/);
        dtxn->WriteColumn(r, 1, buf, TXN_SAFE, TXN_DEFERRED);

        //output[output_index++] = buf;
//        verify(*output_size >= output_index);
//        *output_size = output_index;
        *res = SUCCESS;

    } END_PIE
}

void TpcaPiece::reg_lock_oracles() {
}

} // namespace rococo

