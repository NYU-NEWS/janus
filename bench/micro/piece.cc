#include "all.h"

namespace deptran {

char MICRO_BENCH_TABLE_A[] = "table_a";
char MICRO_BENCH_TABLE_B[] = "table_b";
char MICRO_BENCH_TABLE_C[] = "table_c";
char MICRO_BENCH_TABLE_D[] = "table_d";

void MicroBenchPiece::reg_all() {
    reg_pieces();
    reg_lock_oracles();
}

void MicroBenchPiece::reg_pieces() {
//    TxnRegistry::reg(MICRO_BENCH_W, MICRO_BENCH_W_0, false,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                rrr::i32* res,
//                Value* output,
//                rrr::i32 *output_size) {
//        mdb::Txn *txn = TxnRunner::get_txn(header);
//        verify(input_size == 2);
//        i32 output_index = 0;
//
//        mdb::Row *r = txn->query(txn->get_table(MICRO_BENCH_TABLE_A), input[0]).next();
//        if (!txn->write_column(r, 1, input[1])) {
//            *res = REJECT;
//            *output_size = output_index;
//            return;
//        }
//        verify(*output_size >= output_index);
//        *output_size = output_index;
//        *res = SUCCESS;
//    });
//
//    TxnRegistry::reg(MICRO_BENCH_W, MICRO_BENCH_W_1, false,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                rrr::i32* res,
//                Value* output,
//                rrr::i32 *output_size) {
//        mdb::Txn *txn = TxnRunner::get_txn(header);
//        verify(input_size == 2);
//        i32 output_index = 0;
//
//        mdb::Row *r = txn->query(txn->get_table(MICRO_BENCH_TABLE_B), input[0]).next();
//        if (!txn->write_column(r, 1, input[1])) {
//            *res = REJECT;
//            *output_size = output_index;
//            return;
//        }
//        verify(*output_size >= output_index);
//        *output_size = output_index;
//        *res = SUCCESS;
//    });
//
//    TxnRegistry::reg(MICRO_BENCH_W, MICRO_BENCH_W_2, false,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                rrr::i32* res,
//                Value* output,
//                rrr::i32 *output_size) {
//        mdb::Txn *txn = TxnRunner::get_txn(header);
//        verify(input_size == 2);
//        i32 output_index = 0;
//
//        mdb::Row *r = txn->query(txn->get_table(MICRO_BENCH_TABLE_C), input[0]).next();
//        if (!txn->write_column(r, 1, input[1])) {
//            *res = REJECT;
//            *output_size = output_index;
//            return;
//        }
//        verify(*output_size >= output_index);
//        *output_size = output_index;
//        *res = SUCCESS;
//    });
//
//    TxnRegistry::reg(MICRO_BENCH_W, MICRO_BENCH_W_3, false,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                rrr::i32* res,
//                Value* output,
//                rrr::i32 *output_size) {
//        mdb::Txn *txn = TxnRunner::get_txn(header);
//        verify(input_size == 2);
//        i32 output_index = 0;
//
//        mdb::Row *r = txn->query(txn->get_table(MICRO_BENCH_TABLE_D), input[0]).next();
//        if (!txn->write_column(r, 1, input[1])) {
//            *res = REJECT;
//            *output_size = output_index;
//            return;
//        }
//        verify(*output_size >= output_index);
//        *output_size = output_index;
//        *res = SUCCESS;
//    });
//
//    TxnRegistry::reg(MICRO_BENCH_R, MICRO_BENCH_R_0, false,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                rrr::i32* res,
//                Value* output,
//                rrr::i32 *output_size) {
//        mdb::Txn *txn = TxnRunner::get_txn(header);
//        Value buf;
//        verify(input_size == 1);
//        i32 output_index = 0;
//
//        mdb::Row *r = txn->query(txn->get_table(MICRO_BENCH_TABLE_A), input[0]).next();
//        if (!txn->read_column(r, 1, &buf)) {
//            *res = REJECT;
//            *output_size = output_index;
//            return;
//        }
//        output[output_index++] = buf;
//        verify(*output_size >= output_index);
//        *output_size = output_index;
//        *res = SUCCESS;
//    });
//
//    TxnRegistry::reg(MICRO_BENCH_R, MICRO_BENCH_R_1, false,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                rrr::i32* res,
//                Value* output,
//                rrr::i32 *output_size) {
//        mdb::Txn *txn = TxnRunner::get_txn(header);
//        Value buf;
//        verify(input_size == 1);
//        i32 output_index = 0;
//
//        mdb::Row *r = txn->query(txn->get_table(MICRO_BENCH_TABLE_B), input[0]).next();
//        if (!txn->read_column(r, 1, &buf)) {
//            *res = REJECT;
//            *output_size = output_index;
//            return;
//        }
//        output[output_index++] = buf;
//        verify(*output_size >= output_index);
//        *output_size = output_index;
//        *res = SUCCESS;
//    });
//
//    TxnRegistry::reg(MICRO_BENCH_R, MICRO_BENCH_R_2, false,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                rrr::i32* res,
//                Value* output,
//                rrr::i32 *output_size) {
//        mdb::Txn *txn = TxnRunner::get_txn(header);
//        Value buf;
//        verify(input_size == 1);
//        i32 output_index = 0;
//
//        mdb::Row *r = txn->query(txn->get_table(MICRO_BENCH_TABLE_C), input[0]).next();
//        if (!txn->read_column(r, 1, &buf)) {
//            *res = REJECT;
//            *output_size = output_index;
//            return;
//        }
//        output[output_index++] = buf;
//        verify(*output_size >= output_index);
//        *output_size = output_index;
//        *res = SUCCESS;
//    });
//
//    TxnRegistry::reg(MICRO_BENCH_R, MICRO_BENCH_R_3, false,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                rrr::i32* res,
//                Value* output,
//                rrr::i32 *output_size) {
//        mdb::Txn *txn = TxnRunner::get_txn(header);
//        Value buf;
//        verify(input_size == 1);
//        i32 output_index = 0;
//
//        mdb::Row *r = txn->query(txn->get_table(MICRO_BENCH_TABLE_D), input[0]).next();
//        if (!txn->read_column(r, 1, &buf)) {
//            *res = REJECT;
//            *output_size = output_index;
//            return;
//        }
//        output[output_index++] = buf;
//        verify(*output_size >= output_index);
//        *output_size = output_index;
//        *res = SUCCESS;
//    });
//
}

void MicroBenchPiece::reg_lock_oracles() {
//    TxnRegistry::reg_lock_oracle(MICRO_BENCH_W, MICRO_BENCH_W_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int>* opset
//                ) {
//        verify(input_size == 2);
//        cell_locator cl;
//        cl.tbl_name = MICRO_BENCH_TABLE_A;
//        cl.primary_key = Value(input[0]);
//        cl.col_id = 1;
//        (*opset)[cl] = OP_W;
//    });
//
//    TxnRegistry::reg_lock_oracle(MICRO_BENCH_W, MICRO_BENCH_W_1,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int>* opset
//                ) {
//        verify(input_size == 2);
//        cell_locator cl;
//        cl.tbl_name = MICRO_BENCH_TABLE_B;
//        cl.primary_key = Value(input[0]);
//        cl.col_id = 1;
//        (*opset)[cl] = OP_W;
//    });
//
//    TxnRegistry::reg_lock_oracle(MICRO_BENCH_W, MICRO_BENCH_W_2,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int>* opset
//                ) {
//        verify(input_size == 2);
//        cell_locator cl;
//        cl.tbl_name = MICRO_BENCH_TABLE_C;
//        cl.primary_key = Value(input[0]);
//        cl.col_id = 1;
//        (*opset)[cl] = OP_W;
//    });
//
//    TxnRegistry::reg_lock_oracle(MICRO_BENCH_W, MICRO_BENCH_W_3,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int>* opset
//                ) {
//        verify(input_size == 2);
//        cell_locator cl;
//        cl.tbl_name = MICRO_BENCH_TABLE_D;
//        cl.primary_key = Value(input[0]);
//        cl.col_id = 1;
//        (*opset)[cl] = OP_W;
//    });
//
//    TxnRegistry::reg_lock_oracle(MICRO_BENCH_R, MICRO_BENCH_R_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int>* opset
//                ) {
//        verify(input_size == 1);
//        cell_locator cl;
//        cl.tbl_name = MICRO_BENCH_TABLE_A;
//        cl.primary_key = Value(input[0]);
//        cl.col_id = 1;
//        (*opset)[cl] = OP_IR;
//    });
//
//    TxnRegistry::reg_lock_oracle(MICRO_BENCH_R, MICRO_BENCH_R_1,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int>* opset
//                ) {
//        verify(input_size == 1);
//        cell_locator cl;
//        cl.tbl_name = MICRO_BENCH_TABLE_B;
//        cl.primary_key = Value(input[0]);
//        cl.col_id = 1;
//        (*opset)[cl] = OP_IR;
//    });
//
//    TxnRegistry::reg_lock_oracle(MICRO_BENCH_R, MICRO_BENCH_R_2,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int>* opset
//                ) {
//        verify(input_size == 1);
//        cell_locator cl;
//        cl.tbl_name = MICRO_BENCH_TABLE_C;
//        cl.primary_key = Value(input[0]);
//        cl.col_id = 1;
//        (*opset)[cl] = OP_IR;
//    });
//
//    TxnRegistry::reg_lock_oracle(MICRO_BENCH_R, MICRO_BENCH_R_3,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int>* opset
//                ) {
//        verify(input_size == 1);
//        cell_locator cl;
//        cl.tbl_name = MICRO_BENCH_TABLE_D;
//        cl.primary_key = Value(input[0]);
//        cl.col_id = 1;
//        (*opset)[cl] = OP_IR;
//    });
}

} // namespace deptran
