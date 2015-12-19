#include "all.h"

namespace deptran {

char RW_BENCHMARK_TABLE[] = "customer";

void RWPiece::reg_all() {
    reg_pieces();
    reg_lock_oracles();
}

void RWPiece::reg_pieces() {
  BEGIN_PIE(RW_BENCHMARK_R_TXN, RW_BENCHMARK_R_TXN_0, DF_NO) {
//    mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
    mdb::Txn *txn = dtxn->mdb_txn_;
    Value buf;
    verify(input.size() == 1);
    i32 output_index = 0;

    mdb::Row *r = txn->query(txn->get_table(RW_BENCHMARK_TABLE),
                             input.at(0)).next();
    if (!txn->read_column(r, 1, &buf)) {
        *res = REJECT;
//        *output_size = output_index;
        return;
    }
    output[output_index++] = buf;
//    verify(*output_size >= output_index);
//    *output_size = output_index;
    *res = SUCCESS;
    return;
  } END_PIE

    BEGIN_PIE(RW_BENCHMARK_W_TXN, RW_BENCHMARK_W_TXN_0, DF_REAL) {
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
      mdb::Txn *txn = dtxn->mdb_txn_;
        verify(input.size() == 1);
        i32 output_index = 0;
        Value buf;

        mdb::Row *r = txn->query(txn->get_table(RW_BENCHMARK_TABLE), input[0]).next();
        if (!txn->read_column(r, 1, &buf)) {
            *res = REJECT;
//            *output_size = output_index;
            return;
        }
        buf.set_i64(buf.get_i64() + 1);
        if (!txn->write_column(r, 1, /*input[1]*/buf)) {
            *res = REJECT;
//            *output_size = output_index;
            return;
        }
//        verify(*output_size >= output_index);
//        *output_size = output_index;
        *res = SUCCESS;
        return;
    } END_PIE

}

void RWPiece::reg_lock_oracles() {
//    TxnRegistry::reg_lock_oracle(RW_BENCHMARK_R_TXN, RW_BENCHMARK_R_TXN_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash> *opset
//                ) {
//        verify(input_size == 1);
//        cell_locator_t cl(RW_BENCHMARK_TABLE, 1);
//        cl.primary_key[0] = input[0].get_blob();
//        cl.col_id = 1;
//        (*opset)[cl] |= OP_DR;
//    });
//
//    TxnRegistry::reg_lock_oracle(RW_BENCHMARK_W_TXN, RW_BENCHMARK_W_TXN_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash> *opset
//                ) {
//        verify(input_size == 1);
//        cell_locator_t cl(RW_BENCHMARK_TABLE, 1);
//        cl.primary_key[0] = input[0].get_blob();
//        cl.col_id = 1;
//        (*opset)[cl] |= OP_W;
//    });
//
}

}

