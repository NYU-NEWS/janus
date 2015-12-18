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
        //cell_locator_t cl(TPCA_CUSTOMER, 1);
        mb[0] = input.at(0).get_blob();

        if (!IS_MODE_RCC || (IS_MODE_RCC && IN_PHASE_1)) { // non deptran || deptran start req
            r = txn->query(txn->get_table(TPCA_CUSTOMER), mb).next();
        }

        if (IS_MODE_2PL && output_size == NULL) {
            PieceStatus *ps
                = ((TPLExecutor*)exec)->get_piece_status(header.pid);
            std::function<void(void)> succ = ((TPLExecutor*) exec)->get_2pl_succ_callback(
                    header, input, res, ps);
            std::function<void(void)> fail = ((TPLExecutor*) exec)->get_2pl_fail_callback(
                    header, res, ps);
            ps->reg_rw_lock(std::vector<mdb::column_lock_t>({
                    mdb::column_lock_t(r, 0, ALock::WLOCK)
                    //mdb::column_lock_t(r, 0, ALock::WLOCK),
                    //mdb::column_lock_t(r, 1, ALock::WLOCK),
                    //mdb::column_lock_t(r, 2, ALock::WLOCK),
                    //mdb::column_lock_t(r, 3, ALock::WLOCK),
                    //mdb::column_lock_t(r, 4, ALock::WLOCK),
                    //mdb::column_lock_t(r, 5, ALock::WLOCK),
                    //mdb::column_lock_t(r, 6, ALock::WLOCK),
                    //mdb::column_lock_t(r, 7, ALock::WLOCK),
                    //mdb::column_lock_t(r, 8, ALock::WLOCK),
                    //mdb::column_lock_t(r, 9, ALock::WLOCK),
                    //mdb::column_lock_t(r, 10, ALock::WLOCK),
                    //mdb::column_lock_t(r, 11, ALock::WLOCK),
                    //mdb::column_lock_t(r, 12, ALock::WLOCK),
                    //mdb::column_lock_t(r, 13, ALock::WLOCK),
                    //mdb::column_lock_t(r, 14, ALock::WLOCK),
                    //mdb::column_lock_t(r, 15, ALock::WLOCK),
                    //mdb::column_lock_t(r, 16, ALock::WLOCK),
                    //mdb::column_lock_t(r, 17, ALock::WLOCK),
                    //mdb::column_lock_t(r, 18, ALock::WLOCK),
                    //mdb::column_lock_t(r, 19, ALock::WLOCK)
            }),
                succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 0, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 1, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 2, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 3, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 4, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 5, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 6, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 7, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 8, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 9, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 10, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 11, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 12, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 13, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 14, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 15, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 16, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 17, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 18, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 19, succ, fail);
            return;
        }

        RCC_KISS(r, 1, false);
        RCC_SAVE_ROW(r, TPCC_PAYMENT_1); 
        RCC_PHASE1_RET;
        RCC_LOAD_ROW(r, TPCC_PAYMENT_1);

        txn->read_column(r, 1, &buf);
        buf.set_i64(buf.get_i64() + 1/*input[1].get_i64()*/);
        txn->write_column(r, 1, buf);

        //output[output_index++] = buf;
        verify(*output_size >= output_index);
        *output_size = output_index;
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

        if (!IS_MODE_RCC || (IS_MODE_RCC && IN_PHASE_1)) { // non deptran || deptran start req
            r = txn->query(txn->get_table(TPCA_TELLER), mb).next();
        }

        if (IS_MODE_2PL && output_size == NULL) {
            PieceStatus *ps
                = ((TPLExecutor*)exec)->get_piece_status(header.pid);
            std::function<void(void)> succ = ((TPLExecutor*) exec)->get_2pl_succ_callback(
                    header, input, res, ps);
            std::function<void(void)> fail = ((TPLExecutor*) exec)->get_2pl_fail_callback(
                    header, res, ps);
            ps->reg_rw_lock(std::vector<mdb::column_lock_t>({
                    mdb::column_lock_t(r, 0, ALock::WLOCK)
                    //mdb::column_lock_t(r, 0, ALock::WLOCK),
                    //mdb::column_lock_t(r, 1, ALock::WLOCK),
                    //mdb::column_lock_t(r, 2, ALock::WLOCK),
                    //mdb::column_lock_t(r, 3, ALock::WLOCK),
                    //mdb::column_lock_t(r, 4, ALock::WLOCK),
                    //mdb::column_lock_t(r, 5, ALock::WLOCK),
                    //mdb::column_lock_t(r, 6, ALock::WLOCK),
                    //mdb::column_lock_t(r, 7, ALock::WLOCK),
                    //mdb::column_lock_t(r, 8, ALock::WLOCK),
                    //mdb::column_lock_t(r, 9, ALock::WLOCK),
                    //mdb::column_lock_t(r, 10, ALock::WLOCK),
                    //mdb::column_lock_t(r, 11, ALock::WLOCK),
                    //mdb::column_lock_t(r, 12, ALock::WLOCK),
                    //mdb::column_lock_t(r, 13, ALock::WLOCK),
                    //mdb::column_lock_t(r, 14, ALock::WLOCK),
                    //mdb::column_lock_t(r, 15, ALock::WLOCK),
                    //mdb::column_lock_t(r, 16, ALock::WLOCK),
                    //mdb::column_lock_t(r, 17, ALock::WLOCK),
                    //mdb::column_lock_t(r, 18, ALock::WLOCK),
                    //mdb::column_lock_t(r, 19, ALock::WLOCK)
            }),
                succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 0, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 1, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 2, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 3, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 4, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 5, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 6, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 7, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 8, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 9, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 10, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 11, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 12, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 13, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 14, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 15, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 16, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 17, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 18, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 19, succ, fail);
            return;
        }

        RCC_KISS(r, 1, false);
        RCC_SAVE_ROW(r, TPCA_PAYMENT_2); 
        RCC_PHASE1_RET;
        RCC_LOAD_ROW(r, TPCA_PAYMENT_2); 

        if (!txn->read_column(r, 1, &buf)) {
            *res = REJECT;
            *output_size = output_index;
            Log::debug("Customer id: %d, read failed", input.at(0).get_i32());
            return;
        }
        buf.set_i64(buf.get_i64() + 1/*input[1].get_i64()*/);
        if (!txn->write_column(r, 1, buf)) {
            *res = REJECT;
            *output_size = output_index;
            Log::debug("Customer id: %d, write failed", input.at(0).get_i32());
            return;
        }
        //output[output_index++] = buf;
        verify(*output_size >= output_index);
        *output_size = output_index;
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

        if (!IS_MODE_RCC || (IS_MODE_RCC && IN_PHASE_1)) { // non deptran || deptran start req
            r = txn->query(txn->get_table(TPCA_BRANCH), mb).next();
        }

        if (IS_MODE_2PL && output_size == NULL) {
            PieceStatus *ps
                = ((TPLExecutor*)exec)->get_piece_status(header.pid);
            std::function<void(void)> succ = ((TPLExecutor*) exec)->get_2pl_succ_callback(
                    header, input, res, ps);
            std::function<void(void)> fail = ((TPLExecutor*) exec)->get_2pl_fail_callback(
                    header, res, ps);
            ps->reg_rw_lock(std::vector<mdb::column_lock_t>({
                    mdb::column_lock_t(r, 0, ALock::WLOCK)
                    //mdb::column_lock_t(r, 0, ALock::WLOCK),
                    //mdb::column_lock_t(r, 1, ALock::WLOCK),
                    //mdb::column_lock_t(r, 2, ALock::WLOCK),
                    //mdb::column_lock_t(r, 3, ALock::WLOCK),
                    //mdb::column_lock_t(r, 4, ALock::WLOCK),
                    //mdb::column_lock_t(r, 5, ALock::WLOCK),
                    //mdb::column_lock_t(r, 6, ALock::WLOCK),
                    //mdb::column_lock_t(r, 7, ALock::WLOCK),
                    //mdb::column_lock_t(r, 8, ALock::WLOCK),
                    //mdb::column_lock_t(r, 9, ALock::WLOCK),
                    //mdb::column_lock_t(r, 10, ALock::WLOCK),
                    //mdb::column_lock_t(r, 11, ALock::WLOCK),
                    //mdb::column_lock_t(r, 12, ALock::WLOCK),
                    //mdb::column_lock_t(r, 13, ALock::WLOCK),
                    //mdb::column_lock_t(r, 14, ALock::WLOCK),
                    //mdb::column_lock_t(r, 15, ALock::WLOCK),
                    //mdb::column_lock_t(r, 16, ALock::WLOCK),
                    //mdb::column_lock_t(r, 17, ALock::WLOCK),
                    //mdb::column_lock_t(r, 18, ALock::WLOCK),
                    //mdb::column_lock_t(r, 19, ALock::WLOCK)
            }),
                succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 0, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 1, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 2, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 3, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 4, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 5, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 6, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 7, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 8, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 9, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 10, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 11, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 12, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 13, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 14, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 15, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 16, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 17, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 18, succ, fail);
            //((mdb::Txn2PL *)txn)->reg_write_column(r, 19, succ, fail);
            return;
        }
        
        RCC_KISS(r, 1, false);
        RCC_SAVE_ROW(r, TPCA_PAYMENT_3);
        RCC_PHASE1_RET;
        RCC_LOAD_ROW(r, TPCA_PAYMENT_3);

        if (!txn->read_column(r, 1, &buf)) {
            *res = REJECT;
            *output_size = output_index;
            Log::debug("Customer id: %d, read failed", input.at(0).get_i32());
            return;
        }
        buf.set_i64(buf.get_i64() + 1/*input[1].get_i64()*/);
        if (!txn->write_column(r, 1, buf)) {
            *res = REJECT;
            *output_size = output_index;
            Log::debug("Customer id: %d, write failed", input.at(0).get_i32());
            return;
        }
        //output[output_index++] = buf;
        verify(*output_size >= output_index);
        *output_size = output_index;
        *res = SUCCESS;

    } END_PIE
}

void TpcaPiece::reg_lock_oracles() {
//    TxnRegistry::reg_lock_oracle(TPCA_PAYMENT, TPCA_PAYMENT_1,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//        verify(input_size == 1);
//        cell_locator_t cl(TPCA_CUSTOMER, 1);
//        cl.primary_key[0] = input[0].get_blob();
//        cl.col_id = 1;
//        (*opset)[cl] = OP_W;
//    });
//
//    TxnRegistry::reg_lock_oracle(TPCA_PAYMENT, TPCA_PAYMENT_2,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//        verify(input_size == 1);
//        cell_locator_t cl(TPCA_TELLER, 1);
//        cl.primary_key[0] = input[0].get_blob();
//        cl.col_id = 1;
//        (*opset)[cl] = OP_W;
//    });
//
//    TxnRegistry::reg_lock_oracle(TPCA_PAYMENT, TPCA_PAYMENT_3,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//        verify(input_size == 1);
//        cell_locator_t cl(TPCA_BRANCH, 1);
//        cl.primary_key[0] = input[0].get_blob();
//        cl.col_id = 1;
//        (*opset)[cl] = OP_W;
//    });
}

} // namespace rococo

