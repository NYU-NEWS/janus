#include "all.h"
#include <limits>

namespace deptran {

char TPCC_TB_WAREHOUSE[] =    "warehouse";
char TPCC_TB_DISTRICT[] =     "district";
char TPCC_TB_CUSTOMER[] =     "customer";
char TPCC_TB_HISTORY[] =      "history";
char TPCC_TB_ORDER[] =        "order";
char TPCC_TB_NEW_ORDER[] =    "new_order";
char TPCC_TB_ITEM[] =         "item";
char TPCC_TB_STOCK[] =        "stock";
char TPCC_TB_ORDER_LINE[] =   "order_line";
char TPCC_TB_ORDER_C_ID_SECONDARY[] = "order_secondary";

void TpccPiece::reg_new_order() {
    TxnRegistry::reg(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_0, // Ri & W district
            DF_NO,
            [] (const RequestHeader &header, 
                const Value *input,
                i32 input_size, 
                i32 *res, 
                Value *output,
                i32 *output_size, 
                row_map_t *row_map,
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv,
                std::vector<TxnInfo *> *conflict_txns) {

            verify(row_map == NULL);
            verify(input_size == 2);

            i32 output_index = 0;
            mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
            Value buf;
            mdb::MultiBlob mb(2);
            mb[0] = input[1].get_blob();
            mb[1] = input[0].get_blob();

            mdb::Row *r = txn->query(txn->get_table(TPCC_TB_DISTRICT), mb,
                output_size, header.pid).next();

            if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                && output_size == NULL) {
                mdb::Txn2PL::PieceStatus *ps
                    = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                std::function<void(void)> succ_callback =
                        TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                std::function<void(void)> fail_callback =
                        TPL::get_2pl_fail_callback(header, res, ps);
                ps->reg_rw_lock(
                    std::vector<mdb::column_lock_t>({
                        mdb::column_lock_t(r, 8, ALock::RLOCK),
                        mdb::column_lock_t(r, 10, ALock::WLOCK)
                        }), succ_callback, fail_callback);

                //((mdb::Txn2PL *)txn)->reg_read_column(r, 8, succ_callback,
                //    fail_callback);
                //((mdb::Txn2PL *)txn)->reg_write_column(r, 10, succ_callback,
                //    fail_callback);
                return;
            }

            if (pv) {
                int col_id = 10;
                ((DepRow *)r)->get_dep_entry(col_id)->touch(tv, true);
            }

            // R district
            if (!txn->read_column(r, 8, &buf)) { // read d_tax
                *res = REJECT;
                *output_size = output_index;
                return;
            }
            output[output_index++] = buf;
            if (!txn->read_column(r, 10, &buf)) { // read d_next_o_id
                *res = REJECT;
                *output_size = output_index;
                return;
            }
            output[output_index++] = buf;

            // W district
            buf.set_i32((i32)(buf.get_i32() + 1));
            if (!txn->write_column(r, 10, buf)) { // read d_next_o_id, increament by 1
                *res = REJECT;
                *output_size = output_index;
                return;
            }

            verify(*output_size >= output_index);
            *output_size = output_index;
            *res = SUCCESS;
            Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_0);
            return;
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            //cl.col_id = 8;
//            //(*opset)[cl] = OP_IR; //FIXME optional, since this column will never be written
//
//            cell_locator_t cl(TPCC_TB_DISTRICT, 2, 10);
//            cl.primary_key[0] = input[1].get_blob();
//            cl.primary_key[1] = input[0].get_blob();
//
//            (*opset)[cl] = OP_IR | OP_W; //XXX or simply OP_W in single thread, since the only reason to keep this column is to get an available key for o_id
//            });

    TxnRegistry::reg(
            TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_1, // R warehouse
            DF_NO,
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                verify(row_map == NULL);
                Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_1);
                verify(input_size == 1);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                Value buf;
                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_WAREHOUSE),
                    input[0], output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);
                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 7, ALock::RLOCK)
                            }), succ_callback, fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 7, succ_callback,
                    //    fail_callback);
                    return;
                }

                // R warehouse
                if (!txn->read_column(r, 7, &buf)) { // read w_tax
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf;
                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_1);
                return;
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_1,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            //cell_locator_t cl;
//            //cl.tbl_name = TPCC_TB_WAREHOUSE;
//            //cl.primary_key = MultiValue(std::vector<Value>({
//            //        input[0]
//            //        }));
//
//            //cl.col_id = 7;
//            //(*opset)[cl] = OP_IR; //FIXME optional, since this column will never be written
//            });

    TxnRegistry::reg(
            TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_2, // R customer
            DF_NO, //XXX either i or d is ok
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                verify(row_map == NULL);
                verify(input_size == 3);
                Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_2);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                Value buf;
                mdb::MultiBlob mb(3);
                mb[0] = input[2].get_blob();
                mb[1] = input[1].get_blob();
                mb[2] = input[0].get_blob();
                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_CUSTOMER), mb,
                    output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback =
                            TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback =
                            TPL::get_2pl_fail_callback(header, res, ps);
                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 5, ALock::RLOCK),
                            mdb::column_lock_t(r, 13, ALock::RLOCK),
                            mdb::column_lock_t(r, 15, ALock::RLOCK)
                            }), succ_callback, fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 5, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 13, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 15, succ_callback,
                    //    fail_callback);
                    return;
                }

                // R customer
                if (!txn->read_column(r, 5, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf;

                if (!txn->read_column(r, 13, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf;

                if (!txn->read_column(r, 15, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf;
                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_2);
                return;
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_2,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            //cell_locator_t cl;
//            //cl.tbl_name = TPCC_TB_CUSTOMER;
//            //cl.primary_key = MultiValue(std::vector<Value>({
//            //        input[2],
//            //        input[1],
//            //        input[0]
//            //        }));
//
//            //cl.col_id = 5;
//            //(*opset)[cl] = OP_DR; //FIXME optional
//            //cl.col_id = 13;
//            //(*opset)[cl] = OP_DR; //FIXME ditto
//            //cl.col_id = 15;
//            //(*opset)[cl] = OP_DR; //FIXME ditto
//            });

    TxnRegistry::reg(
            TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_3, // W order
            DF_REAL,
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                verify(input_size == 7);
                Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_3);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::Table *tbl = txn->get_table(TPCC_TB_ORDER);
                mdb::Row *r = NULL;

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::MultiBlob mb(3);
                    mb[0] = input[1].get_blob();
                    mb[1] = input[2].get_blob();
                    mb[2] = input[3].get_blob();
                    r = txn->query(txn->get_table(TPCC_TB_ORDER_C_ID_SECONDARY),
                        mb, false, header.pid).next();

                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback =
                            TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback =
                            TPL::get_2pl_fail_callback(header, res, ps);
                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 3, ALock::WLOCK)
                            }), succ_callback, fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 3, succ_callback,
                    //    fail_callback);
                    return;
                }

                // W order
                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                    std::vector<Value> row_data({
                        input[1],   // o_d_id
                        input[2],   // o_w_id
                        input[0],   // o_id
                        input[3],   // o_c_id
                        Value(std::to_string(time(NULL))),  // o_entry_d
                        input[4],   // o_carrier_id
                        input[5],   // o_ol_cnt
                        input[6]    // o_all_local
                        });

                    switch (DTxnMgr::get_sole_mgr()->get_mode()) {
                        case MODE_2PL:
                            r = mdb::FineLockedRow::create(tbl->schema(), row_data);
                            break;
                        case MODE_OCC:
                        case MODE_NONE:
                            r = mdb::VersionedRow::create(tbl->schema(), row_data);
                            break;
                        case MODE_DEPTRAN:
                            r = DepRow::create(tbl->schema(), row_data);
                            break;
                        default:
                            verify(0);
                    }
                }

                bool do_finish = true;
                if (row_map) { // deptran
                    if (pv) { // start req
                        //std::map<int, entry_t *> &m = DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER][r->get_key()];
                        DepRow *dr = (DepRow *)r;
                        dr->get_dep_entry(0)->touch(tv, false);
                        dr->get_dep_entry(1)->touch(tv, false);
                        dr->get_dep_entry(2)->touch(tv, false);
                        dr->get_dep_entry(5)->touch(tv, false);

                        (*row_map)[TPCC_TB_ORDER][r->get_key()] = r;

                        //cell_locator_t cl(TPCC_TB_ORDER, 3);
                        //cl.primary_key[0] = r->get_blob(0);
                        //cl.primary_key[1] = r->get_blob(1);
                        //cl.primary_key[2] = r->get_blob(2);
                        //cl.col_id = 0;
                        //(*entry_map)[cl] = dr->get_dep_entry(0);
                        //cl.col_id = 1;
                        //(*entry_map)[cl] = dr->get_dep_entry(1);
                        //cl.col_id = 2;
                        //(*entry_map)[cl] = dr->get_dep_entry(2);
                        //cl.col_id = 5;
                        //(*entry_map)[cl] = dr->get_dep_entry(5);
                        do_finish = false;
                    } else { // finish req
                        //mdb::MultiBlob mb(3);
                        //mb[0] = input[1].get_blob();
                        //mb[1] = input[2].get_blob();
                        //mb[2] = input[0].get_blob();
                        //r = (*row_map)[TPCC_TB_ORDER][mb];
                        //verify(r != NULL); //FIXME remove this line after debug

                        //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER].erase(mb);
                        auto kv = row_map->begin()->second.begin();
                        r = kv->second;
                        //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER].erase(kv->first);
                        verify(r != NULL); //FIXME remove this line after debug
                    }
                }

                if (do_finish) { // not deptran
                    if (!txn->insert_row(tbl, r)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }

                    // write TPCC_TB_ORDER_C_ID_SECONDARY
                    mdb::MultiBlob mb(3);
                    mb[0] = input[1].get_blob();
                    mb[1] = input[2].get_blob();
                    mb[2] = input[3].get_blob();
                    r = txn->query(txn->get_table(TPCC_TB_ORDER_C_ID_SECONDARY),
                            mb, true, header.pid).next();
                    if (!txn->write_column(r, 3, input[0])) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }

                    verify(*output_size >= output_index);
                    *output_size = output_index;
                    *res = SUCCESS;
                    Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_3);
                    return;
                }
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_3,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            cell_locator_t cl(TPCC_TB_ORDER, 3);
//            cl.primary_key[0] = input[1].get_blob();
//            cl.primary_key[1] = input[2].get_blob();
//            cl.primary_key[2] = input[0].get_blob();
//
//            //Log::debug("order lock hash: %u", mdb::MultiBlob::hash()(cl.primary_key));
//            cl.col_id = 0;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 1;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 2;
//            (*opset)[cl] = OP_W;
//            //cl.col_id = 3;
//            //(*opset)[cl] = OP_W;
//            //cl.col_id = 4;
//            //(*opset)[cl] = OP_W;
//            cl.col_id = 5;
//            (*opset)[cl] = OP_W;
//            //cl.col_id = 6;
//            //(*opset)[cl] = OP_W;
//            //cl.col_id = 7;
//            //(*opset)[cl] = OP_W;
//            });

    TxnRegistry::reg(
            TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_4, // W new_order
            DF_REAL,
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                verify(input_size == 3);
                Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_4);

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    TPL::get_2pl_proceed_callback(header, input,
                        input_size, res)();
                    return;
                }

                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::Table *tbl = txn->get_table(TPCC_TB_NEW_ORDER);
                mdb::Row *r = NULL;

                // W new_order
                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                    std::vector<Value> row_data({
                        input[1],   // o_d_id
                        input[2],   // o_w_id
                        input[0],   // o_id
                        });

                    switch (DTxnMgr::get_sole_mgr()->get_mode()) {
                        case MODE_2PL:
                            r = mdb::FineLockedRow::create(tbl->schema(), row_data);
                            break;
                        case MODE_OCC:
                        case MODE_NONE:
                            r = mdb::VersionedRow::create(tbl->schema(), row_data);
                            break;
                        case MODE_DEPTRAN:
                            r = DepRow::create(tbl->schema(), row_data);
                            break;
                        default:
                            verify(0);
                    }
                }

                bool do_finish = true;
                if (row_map) { // deptran
                    if (pv) { // start req
                        //std::map<int, entry_t *> &m = DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_NEW_ORDER][r->get_key()];
                        DepRow *dr = (DepRow *)r;
                        dr->get_dep_entry(0)->touch(tv, false);
                        dr->get_dep_entry(1)->touch(tv, false);
                        dr->get_dep_entry(2)->touch(tv, false);

                        (*row_map)[TPCC_TB_NEW_ORDER][r->get_key()] = r;

                        //cell_locator_t cl(TPCC_TB_NEW_ORDER, 3);
                        //cl.primary_key[0] = r->get_blob(0);
                        //cl.primary_key[1] = r->get_blob(1);
                        //cl.primary_key[2] = r->get_blob(2);
                        //cl.col_id = 0;
                        //(*entry_map)[cl] = dr->get_dep_entry(0);
                        //cl.col_id = 1;
                        //(*entry_map)[cl] = dr->get_dep_entry(1);
                        //cl.col_id = 2;
                        //(*entry_map)[cl] = dr->get_dep_entry(2);
                        do_finish = false;
                    } else {
                        //mdb::MultiBlob mb(3);
                        //mb[0] = input[1].get_blob();
                        //mb[1] = input[2].get_blob();
                        //mb[2] = input[0].get_blob();
                        //r = (*row_map)[TPCC_TB_NEW_ORDER][mb];
                        //verify(r != NULL); //FIXME remove this line after debug
                        //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_NEW_ORDER].erase(mb);
                        //do_finish = true;

                        auto kv = row_map->begin()->second.begin();
                        r = kv->second;
                        //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_NEW_ORDER].erase(kv->first);
                        verify(r != NULL); //FIXME remove this line after debug
                    }
                }

                if (do_finish) {
                    if (!txn->insert_row(tbl, r)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    verify(*output_size >= output_index);
                    *output_size = output_index;
                    *res = SUCCESS;
                    Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_4);
                    return;
                }
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_4,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            cell_locator_t cl(TPCC_TB_NEW_ORDER, 3);
//            cl.primary_key[0] = input[1].get_blob();
//            cl.primary_key[1] = input[2].get_blob();
//            cl.primary_key[2] = input[0].get_blob();
//
//            cl.col_id = 0;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 1;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 2;
//            (*opset)[cl] = OP_W;
//            });

    TxnRegistry::reg(
            TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_5, // Ri item
            DF_NO,
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                verify(row_map == NULL);
                verify(input_size == 1);
                Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_5);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                Value buf;
                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_ITEM), input[0],
                    output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {

                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 2, ALock::RLOCK),
                            mdb::column_lock_t(r, 3, ALock::RLOCK),
                            mdb::column_lock_t(r, 4, ALock::RLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 2, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 3, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 4, succ_callback,
                    //    fail_callback);

                    return;
                }

                // Ri item
                if (!txn->read_column(r, 2, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 0 ==> i_name
                if (!txn->read_column(r, 3, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 1 ==> i_price
                if (!txn->read_column(r, 4, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 2 ==> i_data
                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_5);
                return;
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_5,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            //cell_locator_t cl;
//            //cl.tbl_name = TPCC_TB_ITEM;
//            //cl.primary_key = MultiValue(std::vector<Value>({
//            //        input[0]
//            //        }));
//
//            //cl.col_id = 2;
//            //(*opset)[cl] = OP_IR; //FIXME optional
//            //cl.col_id = 3;
//            //(*opset)[cl] = OP_IR; //FIXME ditto
//            //cl.col_id = 4;
//            //(*opset)[cl] = OP_IR; //FIXME ditto
//            });

    TxnRegistry::reg(
            TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_6, // Ri stock
            DF_NO,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(row_map == NULL);
                verify(input_size == 3);
                Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_6);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                Value buf;
                mdb::MultiBlob mb(2);
                mb[0] = input[0].get_blob();
                mb[1] = input[1].get_blob();
                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_STOCK), mb,
                    output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback =
                            TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback =
                            TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 3, ALock::RLOCK),
                            mdb::column_lock_t(r, 16, ALock::RLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 3, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 16, succ_callback,
                    //    fail_callback);
                    return;
                }

                //i32 s_dist_col = 3 + input[2].get_i32();
                // Ri stock
                if (!txn->read_column(r, 3, &buf)) { // FIXME compress all s_dist_xx into one column
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 0 ==> s_dist_xx

                if (!txn->read_column(r, 16, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 1 ==> s_data

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_6);
                return;
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_6,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            //cell_locator_t cl;
//            //cl.tbl_name = TPCC_TB_STOCK;
//            //cl.primary_key = MultiValue(std::vector<Value>({
//            //        input[0],
//            //        input[1]
//            //        }));
//
//            //i32 s_dist_col = 3 + input[3].get_i32();
//            //cl.col_id = s_dist_col;
//            //(*opset)[cl] = OP_IR; //FIXME not required
//            //cl.col_id = 16;
//            //(*opset)[cl] = OP_IR; //FIXME optional, make this in another piece
//            });

    TxnRegistry::reg(
            TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_7, // W stock
            DF_REAL,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(input_size == 4);
                Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_7);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                Value buf;
                mdb::Row *r = NULL;
                mdb::MultiBlob mb(2);
                mb[0] = input[0].get_blob();
                mb[1] = input[1].get_blob();
                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                    r = txn->query(txn->get_table(TPCC_TB_STOCK), mb,
                        output_size, header.pid).next();
                }

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {

                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback =
                            TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback =
                            TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 2, ALock::WLOCK),
                            mdb::column_lock_t(r, 13, ALock::WLOCK),
                            mdb::column_lock_t(r, 14, ALock::WLOCK),
                            mdb::column_lock_t(r, 15, ALock::WLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 2, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 13, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 14, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 15, succ_callback,
                    //    fail_callback);
                    return;
                }

                bool do_finish = true;
                if (row_map) { // deptran
                    if (pv) { // start req
                        (*row_map)[TPCC_TB_STOCK][mb] = r;
                        //Log::debug("stock P1: hv: %u, k: %u, r: %p", mdb::MultiBlob::hash()((*row_map)[TPCC_TB_STOCK].begin()->first), mdb::MultiBlob::hash()(cl.primary_key), r);
                        DepRow *dr = (DepRow *)r;
                        dr->get_dep_entry(2)->touch(tv, false);
                        dr->get_dep_entry(13)->touch(tv, false);
                        dr->get_dep_entry(14)->touch(tv, false);
                        dr->get_dep_entry(15)->touch(tv, false);
                        do_finish = false;
                    } else {
                        //r = (*row_map)[TPCC_TB_STOCK][cl.primary_key];
                        //do_finish = true;

                        r = row_map->begin()->second.begin()->second;
                        verify(r != NULL); //FIXME remove this line after debug
                    }
                }

                if (do_finish) {
                    // Ri stock
                    i32 new_ol_quantity;
                    if (!txn->read_column(r, 2, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    new_ol_quantity = buf.get_i32() - input[2].get_i32();

                    if (!txn->read_column(r, 13, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    Value new_s_ytd(buf.get_i32() + input[2].get_i32());

                    if (!txn->read_column(r, 14, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    Value new_s_order_cnt((i32)(buf.get_i32() + 1));

                    if (!txn->read_column(r, 15, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    Value new_s_remote_cnt(buf.get_i32() + input[3].get_i32());

                    if (new_ol_quantity < 10)
                        new_ol_quantity += 91;
                    Value new_ol_quantity_value(new_ol_quantity);

                    if (!txn->write_columns(r, std::vector<mdb::column_id_t>({
                                2,  // s_quantity
                                13, // s_ytd
                                14, // s_order_cnt
                                15  // s_remote_cnt
                            }), std::vector<Value>({
                                new_ol_quantity_value,
                                new_s_ytd,
                                new_s_order_cnt,
                                new_s_remote_cnt
                            }))) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    verify(*output_size >= output_index);
                    *output_size = output_index;
                    *res = SUCCESS;
                    Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_7);
                    return;
                }
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_7,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            cell_locator_t cl(TPCC_TB_STOCK, 2);
//            cl.primary_key[0] = input[0].get_blob();
//            cl.primary_key[1] = input[1].get_blob();
//
//            //Log::debug("stock lock: hv: %u, ", mdb::MultiBlob::hash()(cl.primary_key));
//            cl.col_id = 2;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 13;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 14;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 15;
//            (*opset)[cl] = OP_W;
//            });

    TxnRegistry::reg(
            TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_8, // W order_line
            DF_REAL,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(input_size == 10);
                Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_8);

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    TPL::get_2pl_proceed_callback(header, input,
                        input_size, res)();
                    return;
                }

                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::Table *tbl = txn->get_table(TPCC_TB_ORDER_LINE);
                mdb::Row *r = NULL;

                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                    std::vector<Value> input_buf(input, input + input_size);

                    switch (DTxnMgr::get_sole_mgr()->get_mode()) {
                        case MODE_2PL:
                            r = mdb::FineLockedRow::create(tbl->schema(), input_buf);
                            break;
                        case MODE_OCC:
                        case MODE_NONE:
                            r = mdb::VersionedRow::create(tbl->schema(), input_buf);
                            break;
                        case MODE_DEPTRAN:
                            r = DepRow::create(tbl->schema(), input_buf);
                            break;
                        default:
                            verify(0);
                    }
                }

                bool do_finish = true;
                if (row_map) { // deptran
                    if (pv) { // start req
                        //std::map<int, entry_t *> &m = DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER_LINE][r->get_key()];
                        DepRow *dr = (DepRow *)r;
                        dr->get_dep_entry(0)->touch(tv, false);
                        dr->get_dep_entry(1)->touch(tv, false);
                        dr->get_dep_entry(2)->touch(tv, false);
                        dr->get_dep_entry(3)->touch(tv, false);
                        dr->get_dep_entry(4)->touch(tv, false);
                        dr->get_dep_entry(6)->touch(tv, false);
                        dr->get_dep_entry(8)->touch(tv, false);
                        (*row_map)[TPCC_TB_ORDER_LINE][r->get_key()] = r;

                        //cell_locator_t cl(TPCC_TB_ORDER_LINE, 4);
                        //cl.primary_key[0] = r->get_blob(0);
                        //cl.primary_key[1] = r->get_blob(1);
                        //cl.primary_key[2] = r->get_blob(2);
                        //cl.primary_key[3] = r->get_blob(3);
                        //cl.col_id = 0;
                        //(*entry_map)[cl] = dr->get_dep_entry(0);
                        //cl.col_id = 1;
                        //(*entry_map)[cl] = dr->get_dep_entry(1);
                        //cl.col_id = 2;
                        //(*entry_map)[cl] = dr->get_dep_entry(2);
                        //cl.col_id = 3;
                        //(*entry_map)[cl] = dr->get_dep_entry(3);
                        //cl.col_id = 4;
                        //(*entry_map)[cl] = dr->get_dep_entry(4);
                        //cl.col_id = 6;
                        //(*entry_map)[cl] = dr->get_dep_entry(6);
                        //cl.col_id = 8;
                        //(*entry_map)[cl] = dr->get_dep_entry(8);

                        do_finish = false;
                    } else { // finish req
                        //mdb::MultiBlob mb(4);
                        //mb[0] = input[0].get_blob();
                        //mb[1] = input[1].get_blob();
                        //mb[2] = input[2].get_blob();
                        //mb[3] = input[3].get_blob();
                        auto kv = row_map->begin()->second.begin();
                        r = kv->second;
                        //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER_LINE].erase(kv->first);
                        verify(r != NULL); //FIXME remove this line after debug
                    }
                }

                if (do_finish) {
                    if (!txn->insert_row(tbl, r)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    verify(*output_size >= output_index);
                    *output_size = output_index;
                    *res = SUCCESS;
                    Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_8);
                    return;
                }
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_NEW_ORDER,
//            TPCC_NEW_ORDER_8,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            cell_locator_t cl(TPCC_TB_ORDER_LINE, 4);
//            cl.primary_key[0] = input[0].get_blob();
//            cl.primary_key[1] = input[1].get_blob();
//            cl.primary_key[2] = input[2].get_blob();
//            cl.primary_key[3] = input[3].get_blob();
//
//            cl.col_id = 0;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 1;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 2;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 3;
//            (*opset)[cl] = OP_W;
//            cl.col_id = 4;
//            (*opset)[cl] = OP_W;
//            //cl.col_id = 5;
//            //(*opset)[cl] = OP_W;
//            cl.col_id = 6;
//            (*opset)[cl] = OP_W;
//            //cl.col_id = 7;
//            //(*opset)[cl] = OP_W;
//            cl.col_id = 8;
//            (*opset)[cl] = OP_W;
//            //cl.col_id = 9;
//            //(*opset)[cl] = OP_W;
//            });
//
}

void TpccPiece::reg_payment() {
    TxnRegistry::reg(
            TPCC_PAYMENT,      // txn
            TPCC_PAYMENT_0,    // piece 0, Ri & W warehouse
            DF_NO, // immediately read
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(row_map == NULL);
                verify(input_size == 2);
                Log::debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_0);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                Value buf;
                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_WAREHOUSE),
                    input[0], output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::                        get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::                        get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 1, ALock::RLOCK),
                            mdb::column_lock_t(r, 2, ALock::RLOCK),
                            mdb::column_lock_t(r, 3, ALock::RLOCK),
                            mdb::column_lock_t(r, 4, ALock::RLOCK),
                            mdb::column_lock_t(r, 5, ALock::RLOCK),
                            mdb::column_lock_t(r, 6, ALock::RLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 1, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 2, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 3, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 4, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 5, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 6, succ_callback,
                    //    fail_callback);
                    return;
                }

                // R warehouse
                if (!txn->read_column(r, 1, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 0 ==> w_name
                if (!txn->read_column(r, 2, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 1 ==> w_street_1
                if (!txn->read_column(r, 3, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 2 ==> w_street_2
                if (!txn->read_column(r, 4, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 3 ==> w_city
                if (!txn->read_column(r, 5, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 4 ==> w_state
                if (!txn->read_column(r, 6, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 5 ==> w_zip

                //if (!txn->read_column(r, 8, &buf)) {
                //    *res = REJECT;
                //    *output_size = output_index;
                //    return;
                //}

                //// W warehouse
                //buf.set_double(buf.get_double() + input[1].get_double()); //FIXME delete
                //if (!txn->write_column(r, 8, buf)) {
                //    *res = REJECT;
                //    *output_size = output_index;
                //    return;
                //}

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_PAYMENT, piece: %d end", TPCC_PAYMENT_0);
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_PAYMENT,
//            TPCC_PAYMENT_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            });

    TxnRegistry::reg(
            TPCC_PAYMENT,      // txn
            TPCC_PAYMENT_1,    // piece 1, Ri district
            DF_NO, // immediately read
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(row_map == NULL);
                verify(input_size == 2);
                Log::debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_1);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                Value buf;
                mdb::MultiBlob mb(2);
                mb[0] = input[1].get_blob();
                mb[1] = input[0].get_blob();
                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_DISTRICT), mb,
                    output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 2, ALock::RLOCK),
                            mdb::column_lock_t(r, 3, ALock::RLOCK),
                            mdb::column_lock_t(r, 4, ALock::RLOCK),
                            mdb::column_lock_t(r, 5, ALock::RLOCK),
                            mdb::column_lock_t(r, 6, ALock::RLOCK),
                            mdb::column_lock_t(r, 7, ALock::RLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 2, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 3, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 4, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 5, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 6, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 7, succ_callback,
                    //    fail_callback);
                    return;
                }

                // R district
                if (!txn->read_column(r, 2, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[0] ==> d_name

                if (!txn->read_column(r, 3, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 1 ==> d_street_1

                if (!txn->read_column(r, 4, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 2 ==> d_street_2

                if (!txn->read_column(r, 5, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 3 ==> d_city

                if (!txn->read_column(r, 6, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 4 ==> d_state

                if (!txn->read_column(r, 7, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // 5 ==> d_zip

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_PAYMENT, piece: %d end", TPCC_PAYMENT_1);
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_PAYMENT,
//            TPCC_PAYMENT_1,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            });

    TxnRegistry::reg(
            TPCC_PAYMENT,      // txn
            TPCC_PAYMENT_2,    // piece 1, Ri & W district
            DF_REAL,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(input_size == 3);
                Log::debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_2);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                Value buf;
                mdb::Row *r = NULL;
                mdb::MultiBlob mb(2);
                //cell_locator_t cl(TPCC_TB_DISTRICT, 2);
                mb[0] = input[1].get_blob();
                mb[1] = input[0].get_blob();
                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                    r = txn->query(txn->get_table(TPCC_TB_DISTRICT), mb,
                        output_size, header.pid).next();
                }

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 9, ALock::WLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 9, succ_callback,
                    //    fail_callback);
                    return;
                }

                bool do_finish = true;
                if (row_map) { // deptran
                    if (pv) { // start req
                        (*row_map)[TPCC_TB_DISTRICT][mb] = r;
                        DepRow *dr = (DepRow *)r;
                        dr->get_dep_entry(9)->touch(tv, false);
                        do_finish = false;
                    } else {
                        //std::unordered_map<mdb::MultiBlob, mdb::Row *, mdb::MultiBlob::hash>::iterator it = (*row_map)[TPCC_TB_DISTRICT].find(cl.primary_key);
                        //verify(it != (*row_map)[TPCC_TB_STOCK].end()); //FIXME remove this line after debug
                        //r = (*row_map)[TPCC_TB_DISTRICT][cl.primary_key];
                        r = row_map->begin()->second.begin()->second;
                        verify(r != NULL); //FIXME remove this line after debug
                        do_finish = true;
                    }
                }

                if (do_finish) {
                    if (!txn->read_column(r, 9, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }

                    // W district
                    buf.set_double(buf.get_double() + input[2].get_double());
                    if (!txn->write_column(r, 9, buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }

                    verify(*output_size >= output_index);
                    *output_size = output_index;
                    *res = SUCCESS;
                    Log::debug("TPCC_PAYMENT, piece: %d end", TPCC_PAYMENT_2);
                }
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_PAYMENT,
//            TPCC_PAYMENT_2,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            cell_locator_t cl(TPCC_TB_DISTRICT, 2);
//            cl.primary_key[0] = input[1].get_blob();
//            cl.primary_key[1] = input[0].get_blob();
//
//            cl.col_id = 9;
//            (*opset)[cl] = OP_W;
//            });

    TxnRegistry::reg(
            TPCC_PAYMENT,      // txn
            TPCC_PAYMENT_3,    // piece 2, R customer secondary index, c_last -> c_id
            DF_NO,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(row_map == NULL);
                verify(input_size == 3);
                Log::debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_3);

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    TPL::get_2pl_proceed_callback(header, input,
                        input_size, res)();
                    return;
                }

                i32 output_index = 0;
                /*mdb::Txn *txn = */DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::MultiBlob mbl(3), mbh(3);
                mbl[0] = input[2].get_blob();
                mbh[0] = input[2].get_blob();
                mbl[1] = input[1].get_blob();
                mbh[1] = input[1].get_blob();
                Value c_id_low(std::numeric_limits<i32>::min()), c_id_high(std::numeric_limits<i32>::max());
                mbl[2] = c_id_low.get_blob();
                mbh[2] = c_id_high.get_blob();
                c_last_id_t key_low(input[0].get_str(), mbl, &g_c_last_schema);
                c_last_id_t key_high(input[0].get_str(), mbh, &g_c_last_schema);
                std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high, it_mid;
                bool inc = false, mid_set = false;
                it_low = g_c_last2id.lower_bound(key_low);
                it_high = g_c_last2id.upper_bound(key_high);
                int n_c = 0;
                for (it = it_low; it != it_high; it++) {
                    n_c++;
                    if (mid_set)
                        if (inc) {
                            it_mid++;
                            inc = false;
                        }
                        else
                            inc = true;
                    else {
                        mid_set = true;
                        it_mid = it;
                    }
                }
                Log::debug("w_id: %d, d_id: %d, c_last: %s, num customer: %d", input[1].get_i32(), input[2].get_i32(), input[0].get_str().c_str(), n_c);
                verify(mid_set);
                output[output_index++] = Value(it_mid->second);

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_PAYMENT, piece: %d, end", TPCC_PAYMENT_3);
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_PAYMENT,
//            TPCC_PAYMENT_3,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            });

    TxnRegistry::reg(
            TPCC_PAYMENT,      // txn
            TPCC_PAYMENT_4,    // piece 4, R & W customer
            DF_REAL,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(input_size == 6);
                Log::debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_4);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::Row *r = NULL;
                std::vector<Value> buf;

                mdb::MultiBlob mb(3);
                //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
                mb[0] = input[0].get_blob();
                mb[1] = input[2].get_blob();
                mb[2] = input[1].get_blob();
                // R customer
                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                    r = txn->query(txn->get_table(TPCC_TB_CUSTOMER), mb,
                        output_size, header.pid).next();
                }

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ALock::type_t lock_20_type = ALock::RLOCK;
                    if (input[0].get_i32() % 10 == 0)
                        lock_20_type = ALock::WLOCK;
                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 3, ALock::RLOCK),
                            mdb::column_lock_t(r, 4, ALock::RLOCK),
                            mdb::column_lock_t(r, 5, ALock::RLOCK),
                            mdb::column_lock_t(r, 6, ALock::RLOCK),
                            mdb::column_lock_t(r, 7, ALock::RLOCK),
                            mdb::column_lock_t(r, 8, ALock::RLOCK),
                            mdb::column_lock_t(r, 9, ALock::RLOCK),
                            mdb::column_lock_t(r, 10, ALock::RLOCK),
                            mdb::column_lock_t(r, 11, ALock::RLOCK),
                            mdb::column_lock_t(r, 12, ALock::RLOCK),
                            mdb::column_lock_t(r, 13, ALock::RLOCK),
                            mdb::column_lock_t(r, 14, ALock::RLOCK),
                            mdb::column_lock_t(r, 15, ALock::RLOCK),
                            mdb::column_lock_t(r, 16, ALock::WLOCK),
                            mdb::column_lock_t(r, 17, ALock::WLOCK),
                            mdb::column_lock_t(r, 20, lock_20_type)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 3, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 4, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 5, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 6, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 7, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 8, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 9, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 10, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 11, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 12, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 13, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 14, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 15, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 16, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 17, succ_callback,
                    //    fail_callback);
                    //if (input[0].get_i32() % 10 == 0)
                    //    ((mdb::Txn2PL *)txn)->reg_write_column(r, 20,
                    //        succ_callback, fail_callback);
                    //else
                    //    ((mdb::Txn2PL *)txn)->reg_read_column(r, 20,
                    //        succ_callback, fail_callback);
                    return;
                }

                bool do_finish = true;

                if (row_map) { // deptran
                    if (pv) { // start req
                        (*row_map)[TPCC_TB_CUSTOMER][mb] = r;
                        DepRow *dr = (DepRow *)r;
                        dr->get_dep_entry(16)->touch(tv, false);
                        dr->get_dep_entry(17)->touch(tv, false);
                        dr->get_dep_entry(20)->touch(tv, false);
                        do_finish = false;
                    } else {
                        //std::unordered_map<mdb::MultiBlob, mdb::Row *, mdb::MultiBlob::hash>::iterator it = (*row_map)[TPCC_TB_CUSTOMER].find(cl.primary_key);
                        //verify(it != (*row_map)[TPCC_TB_CUSTOMER].end()); //FIXME remove this line after debug
                        r = row_map->begin()->second.begin()->second;
                        //r = (*row_map)[TPCC_TB_CUSTOMER][cl.primary_key];
                        verify(r != NULL); //FIXME remove this line after debug
                        do_finish = true;
                    }
                }

                if (do_finish) {
                    if (!txn->read_columns(r, std::vector<mdb::column_id_t>({
                                3,  // c_first          buf[0]
                                4,  // c_middle         buf[1]
                                5,  // c_last           buf[2]
                                6,  // c_street_1       buf[3]
                                7,  // c_street_2       buf[4]
                                8,  // c_city           buf[5]
                                9,  // c_state          buf[6]
                                10, // c_zip            buf[7]
                                11, // c_phone          buf[8]
                                12, // c_since          buf[9]
                                13, // c_credit         buf[10]
                                14, // c_credit_lim     buf[11]
                                15, // c_discount       buf[12]
                                16, // c_balance        buf[13]
                                17, // c_ytd_payment    buf[14]
                                20  // c_data           buf[15]
                            }), &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }

                    // if c_credit == "BC" (bad) 10%
                    // here we use c_id to pick up 10% instead of c_credit
                    if (input[0].get_i32() % 10 == 0) {
                        Value c_data((
                                    to_string(input[0])
                                  + to_string(input[2])
                                  + to_string(input[1])
                                  + to_string(input[5])
                                  + to_string(input[4])
                                  + to_string(input[3])
                                  + buf[15].get_str()
                                 ).substr(0, 500));
                        std::vector<mdb::column_id_t> col_ids({
                                16, // c_balance
                                17, // c_ytd_payment
                                20  // c_data
                                });
                        std::vector<Value> col_data({
                                Value(buf[13].get_double() - input[3].get_double()),
                                Value(buf[14].get_double() + input[3].get_double()),
                                c_data
                                });
                        if (!txn->write_columns(r, col_ids, col_data)) {
                            *res = REJECT;
                            *output_size = output_index;
                            return;
                        }
                    }
                    else {
                        std::vector<mdb::column_id_t> col_ids({
                                16, // c_balance
                                17  // c_ytd_payment
                                });
                        std::vector<Value> col_data({
                                Value(buf[13].get_double() - input[3].get_double()),
                                Value(buf[14].get_double() + input[3].get_double())
                                });
                        if (!txn->write_columns(r, col_ids, col_data)) {
                            *res = REJECT;
                            *output_size = output_index;
                            return;
                        }
                    }

                    output[output_index++] = input[0];  // output[0]  =>  c_id
                    output[output_index++] = buf[0];    // output[1]  =>  c_first
                    output[output_index++] = buf[1];    // output[2]  =>  c_middle
                    output[output_index++] = buf[2];    // output[3]  =>  c_last
                    output[output_index++] = buf[3];    // output[4]  =>  c_street_1
                    output[output_index++] = buf[4];    // output[5]  =>  c_street_2
                    output[output_index++] = buf[5];    // output[6]  =>  c_city
                    output[output_index++] = buf[6];    // output[7]  =>  c_state
                    output[output_index++] = buf[7];    // output[8]  =>  c_zip
                    output[output_index++] = buf[8];    // output[9]  =>  c_phone
                    output[output_index++] = buf[9];    // output[10] =>  c_since
                    output[output_index++] = buf[10];   // output[11] =>  c_credit
                    output[output_index++] = buf[11];   // output[12] =>  c_credit_lim
                    output[output_index++] = buf[12];   // output[13] =>  c_discount
                    output[output_index++] = Value(buf[13].get_double() - input[3].get_double()); // output[14] =>  c_balance

                    verify(*output_size >= output_index);
                    *output_size = output_index;
                    *res = SUCCESS;
                    Log::debug("TPCC_PAYMENT, piece: %d end", TPCC_PAYMENT_4);
                }

                });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_PAYMENT,
//            TPCC_PAYMENT_4,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
//            cl.primary_key[0] = input[0].get_blob();
//            cl.primary_key[1] = input[2].get_blob();
//            cl.primary_key[2] = input[1].get_blob();
//
//            cl.col_id = 16; // c_balance
//            (*opset)[cl] = /*OP_DR |*/ OP_W;
//            cl.col_id = 17; // c_ytd_payment
//            (*opset)[cl] = OP_W;
//            cl.col_id = 20; // c_data
//            (*opset)[cl] = OP_W;
//            });

    TxnRegistry::reg(
            TPCC_PAYMENT,      // txn
            TPCC_PAYMENT_5,    // piece 4, W histroy
            DF_REAL,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(input_size == 9);
                Log::debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_5);

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    TPL::get_2pl_proceed_callback(header, input,
                        input_size, res)();
                    return;
                }

                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::Table *tbl = txn->get_table(TPCC_TB_HISTORY);

                // insert history
                mdb::Row *r = NULL;
                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                }

                bool do_finish = true;

                if (row_map) { // deptran
                    if (pv) { // start req
                        do_finish = false;
                    } else { // finish req
                    }
                }

                if (do_finish) {
                    std::vector<Value> row_data(9);
                    row_data[0] = input[0];             // h_key
                    row_data[1] = input[5];             // h_c_id   =>  c_id
                    row_data[2] = input[7];             // h_c_d_id =>  c_d_id
                    row_data[3] = input[6];             // h_c_w_id =>  c_w_id
                    row_data[4] = input[4];             // h_d_id   =>  d_id
                    row_data[5] = input[3];             // h_d_w_id =>  d_w_id
                    row_data[6] = Value(std::to_string(time(NULL)));    // h_date
                    row_data[7] = input[8];             // h_amount =>  h_amount
                    row_data[8] = Value(input[1].get_str() + "    " + input[2].get_str()); // d_data => w_name + 4spaces + d_name
                    switch (DTxnMgr::get_sole_mgr()->get_mode()) {
                        case MODE_2PL:
                            r = mdb::FineLockedRow::create(tbl->schema(), row_data);
                            break;
                        case MODE_OCC:
                        case MODE_NONE:
                            r = mdb::VersionedRow::create(tbl->schema(), row_data);
                            break;
                        case MODE_DEPTRAN:
                            r = DepRow::create(tbl->schema(), row_data);
                            break;
                        default:
                            verify(0);
                    }
                    if (!txn->insert_row(tbl, r)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }

                    verify(*output_size >= output_index);
                    *output_size = output_index;
                    *res = SUCCESS;
                    Log::debug("TPCC_PAYMENT, piece: %d end", TPCC_PAYMENT_5);
                }
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_PAYMENT,
//            TPCC_PAYMENT_5,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            });

}

void TpccPiece::reg_order_status() {
    TxnRegistry::reg(
            TPCC_ORDER_STATUS, // RO
            TPCC_ORDER_STATUS_0, // piece 0, R customer secondary index, c_last -> c_id
            DF_NO,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(row_map == NULL);
                verify(input_size == 3);
                Log::debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_0);

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    TPL::get_2pl_proceed_callback(header, input,
                        input_size, res)();
                    return;
                }

                i32 output_index = 0;
                /*mdb::Txn *txn = */DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::MultiBlob mbl(3), mbh(3);
                mbl[0] = input[2].get_blob();
                mbh[0] = input[2].get_blob();
                mbl[1] = input[1].get_blob();
                mbh[1] = input[1].get_blob();
                Value c_id_low(std::numeric_limits<i32>::min()), c_id_high(std::numeric_limits<i32>::max());
                mbl[2] = c_id_low.get_blob();
                mbh[2] = c_id_high.get_blob();
                c_last_id_t key_low(input[0].get_str(), mbl, &g_c_last_schema);
                c_last_id_t key_high(input[0].get_str(), mbh, &g_c_last_schema);
                std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high, it_mid;
                bool inc = false, mid_set = false;
                it_low = g_c_last2id.lower_bound(key_low);
                it_high = g_c_last2id.upper_bound(key_high);
                int n_c = 0;
                for (it = it_low; it != it_high; it++) {
                    n_c++;
                    if (mid_set)
                        if (inc) {
                            it_mid++;
                            inc = false;
                        }
                        else
                            inc = true;
                    else {
                        mid_set = true;
                        it_mid = it;
                    }
                }
                Log::debug("w_id: %d, d_id: %d, c_last: %s, num customer: %d", input[1].get_i32(), input[2].get_i32(), input[0].get_str().c_str(), n_c);
                verify(mid_set);
                output[output_index++] = Value(it_mid->second);

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_ORDER_STATUS, piece: %d, end", TPCC_ORDER_STATUS_0);
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_ORDER_STATUS,
//            TPCC_ORDER_STATUS_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            });

    TxnRegistry::reg(
            TPCC_ORDER_STATUS, // RO
            TPCC_ORDER_STATUS_1, // Ri customer
            DF_NO,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(row_map == NULL);
                Log::debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_1);
                verify(input_size == 3);
                i32 output_index = 0;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::Table *tbl = txn->get_table(TPCC_TB_CUSTOMER);
                // R customer
                Value buf;
                mdb::MultiBlob mb(3);
                //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
                mb[0] = input[2].get_blob();
                mb[1] = input[1].get_blob();
                mb[2] = input[0].get_blob();
                mdb::Row *r = txn->query(tbl, mb, output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 3, ALock::RLOCK),
                            mdb::column_lock_t(r, 4, ALock::RLOCK),
                            mdb::column_lock_t(r, 5, ALock::RLOCK),
                            mdb::column_lock_t(r, 16, ALock::RLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 3, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 4, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 5, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 16, succ_callback,
                    //    fail_callback);
                    return;
                }

                if (conflict_txns) {
                    ((DepRow *)r)->get_dep_entry(16)->ro_touch(conflict_txns);
                }

                if (!txn->read_column(r, 3, &buf)) { // read c_first
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[0] ==>  c_first
                if (!txn->read_column(r, 4, &buf)) { // read c_middle
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[1] ==>  c_middle
                if (!txn->read_column(r, 5, &buf)) { // read c_last
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[2] ==> c_last
                if (!txn->read_column(r, 16, &buf)) { // read c_balance
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[3] ==> c_balance

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_ORDER_STATUS, piece: %d, end", TPCC_ORDER_STATUS_1);

            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_ORDER_STATUS,
//            TPCC_ORDER_STATUS_1,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//                cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
//                cl.primary_key[0] = input[2].get_blob();
//                cl.primary_key[1] = input[1].get_blob();
//                cl.primary_key[2] = input[0].get_blob();
//
//                cl.col_id = 16; // c_balance
//                (*opset)[cl] = OP_IR;
//            });

    TxnRegistry::reg(
            TPCC_ORDER_STATUS, // RO
            TPCC_ORDER_STATUS_2, // Ri order
            DF_NO,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(row_map == NULL);
                Log::debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_2);
                verify(input_size == 3);
                i32 output_index = 0;
                Value buf;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);

                mdb::MultiBlob mb_0(3);
                mb_0[0] = input[1].get_blob();
                mb_0[1] = input[0].get_blob();
                mb_0[2] = input[2].get_blob();
                mdb::Row *r_0 = txn->query(
                    txn->get_table(TPCC_TB_ORDER_C_ID_SECONDARY), mb_0,
                    output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {

                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);

                    std::function<void(
                            const RequestHeader &,
                            const Value *,
                            rrr::i32,
                            rrr::i32 *)> func =
                                [r_0, txn, ps] (const RequestHeader &header,
                                    const Value *input,
                                    rrr::i32 input_size,
                                    rrr::i32 *res) {

                                    //txn->read_column(r_0, 3, &buf);
                                    mdb::MultiBlob mb(3);
                                    mb[0] = input[1].get_blob();
                                    mb[1] = input[0].get_blob();
                                    mb[2] = r_0->get_blob(3);

                                    mdb::Row *r = txn->query(
                                            txn->get_table(TPCC_TB_ORDER), mb,
                                            false, header.pid).next();

                                    std::function<void(void)> succ_callback1
                                        = TPL::get_2pl_succ_callback(
                                                header, input, input_size, res,
                                                ps);
                                    std::function<void(void)> fail_callback1
                                        = TPL::get_2pl_fail_callback(
                                                header, res, ps);
                                    ps->set_num_waiting_locks(1);

                                    Log::debug("PS1: %p", ps);

                                    ps->reg_rw_lock(
                                        std::vector<mdb::column_lock_t>({
                                            mdb::column_lock_t(r, 2, ALock::RLOCK),
                                            mdb::column_lock_t(r, 4, ALock::RLOCK),
                                            mdb::column_lock_t(r, 5, ALock::RLOCK)
                                            }), succ_callback1, fail_callback1);
                                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 2,
                                    //    succ_callback1, fail_callback1);
                                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 4,
                                    //    succ_callback1, fail_callback1);
                                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 5,
                                    //    succ_callback1, fail_callback1);

                                    return;
                                };

                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res,
                                ps, func);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r_0, 3, ALock::RLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r_0, 3, succ_callback,
                    //    fail_callback);
                    return;
                }


                if (!txn->read_column(r_0, 3, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }

                mdb::MultiBlob mb(3);
                //cell_locator_t cl(TPCC_TB_ORDER, 3);
                mb[0] = input[1].get_blob();
                mb[1] = input[0].get_blob();
                mb[2] = r_0->get_blob(3);

                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_ORDER), mb,
                        true, header.pid).next();

                if (conflict_txns) {
                    ((DepRow *)r)->get_dep_entry(5)->ro_touch(conflict_txns);
                }

                if (!txn->read_column(r, 2, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[0] ==> o_id
                Log::debug("piece: %d, o_id: %d", TPCC_ORDER_STATUS_2, buf.get_i32());
                if (!txn->read_column(r, 4, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[1] ==> o_entry_d
                if (!txn->read_column(r, 5, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[2] ==> o_carrier_id

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_ORDER_STATUS, piece: %d, end", TPCC_ORDER_STATUS_2);

            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_ORDER_STATUS,
//            TPCC_ORDER_STATUS_2,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//                verify(input_size == 3);
//                Value buf;
//                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
//
//                mdb::MultiBlob mb_0(3);
//                mb_0[0] = input[1].get_blob();
//                mb_0[1] = input[0].get_blob();
//                mb_0[2] = input[2].get_blob();
//                mdb::Row *r_0 = txn->query(txn->get_table(TPCC_TB_ORDER_C_ID_SECONDARY), mb_0).next();
//                cell_locator_t cl(TPCC_TB_ORDER, 3, 5);
//                cl.primary_key[0] = input[1].get_blob();
//                cl.primary_key[1] = input[0].get_blob();
//                cl.primary_key[2] = r_0->get_blob(3);
//                (*opset)[cl] = OP_IR;
//            });

    TxnRegistry::reg(
            TPCC_ORDER_STATUS, // RO
            TPCC_ORDER_STATUS_3, // R order_line
            DF_NO,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                verify(row_map == NULL);
                Log::debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_3);
                verify(input_size == 3);
                i32 output_index = 0;
                Value buf;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::MultiBlob mbl(4), mbh(4);
                Log::debug("ol_d_id: %d, ol_w_id: %d, ol_o_id: %d",
                    input[2].get_i32(), input[1].get_i32(), input[0].get_i32());
                mbl[0] = input[1].get_blob();
                mbh[0] = input[1].get_blob();
                mbl[1] = input[0].get_blob();
                mbh[1] = input[0].get_blob();
                mbl[2] = input[2].get_blob();
                mbh[2] = input[2].get_blob();
                Value ol_number_low(std::numeric_limits<i32>::min()),
                      ol_number_high(std::numeric_limits<i32>::max());
                mbl[3] = ol_number_low.get_blob();
                mbh[3] = ol_number_high.get_blob();

                mdb::ResultSet rs = txn->query_in(
                        txn->get_table(TPCC_TB_ORDER_LINE), mbl, mbh,
                        output_size, header.pid, mdb::ORD_DESC);
                mdb::Row *r = NULL;
                //cell_locator_t cl(TPCC_TB_ORDER_LINE, 4);
                //cl.primary_key[0] = input[2].get_blob();
                //cl.primary_key[1] = input[1].get_blob();
                //cl.primary_key[2] = input[0].get_blob();
                std::vector<mdb::Row *> row_list;
                row_list.reserve(15);
                while (rs.has_next()) {
                    row_list.push_back(rs.next());
                }

                verify(row_list.size() != 0);

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);


                    std::vector<mdb::column_lock_t> column_locks;
                    column_locks.reserve(5 * row_list.size());

                    int i = 0;
                    Log::debug("row_list size: %u", row_list.size());
                    while (i < row_list.size()) {
                        r = row_list[i++];

                        column_locks.push_back(
                                mdb::column_lock_t(r, 4, ALock::RLOCK)
                                );
                        column_locks.push_back(
                                mdb::column_lock_t(r, 5, ALock::RLOCK)
                                );
                        column_locks.push_back(
                                mdb::column_lock_t(r, 6, ALock::RLOCK)
                                );
                        column_locks.push_back(
                                mdb::column_lock_t(r, 7, ALock::RLOCK)
                                );
                        column_locks.push_back(
                                mdb::column_lock_t(r, 8, ALock::RLOCK)
                                );

                        //((mdb::Txn2PL *)txn)->reg_read_column(r, 4, succ_callback,
                        //    fail_callback);
                        //((mdb::Txn2PL *)txn)->reg_read_column(r, 5, succ_callback,
                        //    fail_callback);
                        //((mdb::Txn2PL *)txn)->reg_read_column(r, 6, succ_callback,
                        //    fail_callback);
                        //((mdb::Txn2PL *)txn)->reg_read_column(r, 7, succ_callback,
                        //    fail_callback);
                        //((mdb::Txn2PL *)txn)->reg_read_column(r, 8, succ_callback,
                        //    fail_callback);
                    }

                    ps->reg_rw_lock(column_locks, succ_callback, fail_callback);

                    return;
                }


                int i = 0;
                while (i < row_list.size()) {
                    r = row_list[i++];

                    if (conflict_txns) {
                        ((DepRow *)r)->get_dep_entry(6)->ro_touch(conflict_txns);
                    }

                    if (!txn->read_column(r, 4, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    output[output_index++] = buf; // output[0] ==> ol_i_id
                    if (!txn->read_column(r, 5, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    output[output_index++] = buf; // output[1] ==> ol_supply_w_id
                    if (!txn->read_column(r, 6, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    output[output_index++] = buf; // output[2] ==> ol_delivery_d
                    if (!txn->read_column(r, 7, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    output[output_index++] = buf; // output[3] ==> ol_quantity
                    if (!txn->read_column(r, 8, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    output[output_index++] = buf; // output[4] ==> ol_amount
                }

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_ORDER_STATUS, piece: %d, end", TPCC_ORDER_STATUS_3);

            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_ORDER_STATUS,
//            TPCC_ORDER_STATUS_3,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//
//                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
//                mdb::MultiBlob mbl(4), mbh(4);
//                mbl[0] = input[2].get_blob();
//                mbh[0] = input[2].get_blob();
//                mbl[1] = input[1].get_blob();
//                mbh[1] = input[1].get_blob();
//                mbl[2] = input[0].get_blob();
//                mbh[2] = input[0].get_blob();
//                Value ol_number_low(std::numeric_limits<i32>::min()), ol_number_high(std::numeric_limits<i32>::max());
//                mbl[3] = ol_number_low.get_blob();
//                mbh[3] = ol_number_high.get_blob();
//
//                mdb::ResultSet rs = txn->query_in(txn->get_table(TPCC_TB_ORDER_LINE), mbl, mbh, mdb::ORD_DESC);
//                mdb::Row *r = NULL;
//                cell_locator_t cl(TPCC_TB_ORDER_LINE, 4, 6);
//                cl.primary_key[0] = input[2].get_blob();
//                cl.primary_key[1] = input[1].get_blob();
//                cl.primary_key[2] = input[0].get_blob();
//                while (rs.has_next()) {
//                    r = rs.next();
//                    cl.primary_key[3] = r->get_blob(3);
//                    (*opset)[cl] = OP_IR;
//                }
//
//            });

}

void TpccPiece::reg_delivery() {
    TxnRegistry::reg(
            TPCC_DELIVERY,
            TPCC_DELIVERY_0, // Ri & W new_order
            DF_FAKE,
            // this is a little bit tricky, the first half will do most of the job,
            // removing the row from the table, but it won't actually release the
            // resource. And the bottom half is in charge of release the resource,
            // including the vertex entry
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_0);
                verify(input_size == 2);
                i32 output_index = 0;
                Value buf;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                //cell_locator_t cl(TPCC_TB_NEW_ORDER, 3);
                mdb::Row *r = NULL;
                mdb::Table *tbl = txn->get_table(TPCC_TB_NEW_ORDER);
                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                    mdb::MultiBlob mbl(3), mbh(3);
                    mbl[0] = input[1].get_blob();
                    mbh[0] = input[1].get_blob();
                    mbl[1] = input[0].get_blob();
                    mbh[1] = input[0].get_blob();
                    Value no_o_id_low(std::numeric_limits<i32>::min()),
                          no_o_id_high(std::numeric_limits<i32>::max());
                    mbl[2] = no_o_id_low.get_blob();
                    mbh[2] = no_o_id_high.get_blob();

                    mdb::ResultSet rs = txn->query_in(tbl, mbl, mbh,
                            output_size, header.pid, mdb::ORD_ASC);

                    if (rs.has_next()) {
                        r = rs.next();

                        if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                            && output_size == NULL) {
                            mdb::Txn2PL::PieceStatus *ps
                                = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                            std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                            std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                            ps->reg_rm_lock(r, succ_callback, fail_callback);

                            //((mdb::Txn2PL *)txn)->reg_write_column(r, 0, succ_callback,
                            //    fail_callback);
                            //((mdb::Txn2PL *)txn)->reg_write_column(r, 1, succ_callback,
                            //    fail_callback);
                            //((mdb::Txn2PL *)txn)->reg_write_column(r, 2, succ_callback,
                            //    fail_callback);
                            return;
                        }

                        if (!txn->read_column(r, 2, &buf)) {
                            *res = REJECT;
                            *output_size = output_index;
                            return;
                        }
                        output[output_index++] = buf;
                    }
                    else {
                        if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                            && output_size == NULL) {
                            TPL::get_2pl_proceed_callback(header, input,
                                input_size, res)();
                            return;
                        }

                        output[output_index++] = Value((i32)-1);

                    }
                }

                if (row_map) { // deptran
                    if (pv) { // deptran start req, top half
                        if (r) { // if find a row
                            //cl.primary_key[0] = input[1].get_blob();
                            //cl.primary_key[1] = input[0].get_blob();
                            //cl.primary_key[2] = r->get_blob(2);

                            DepRow *dr = (DepRow *)r;

                            (*row_map)[TPCC_TB_NEW_ORDER][r->get_key()] = r;
                            dr->get_dep_entry(0)->touch(tv, true);
                            dr->get_dep_entry(1)->touch(tv, true);
                            dr->get_dep_entry(2)->touch(tv, true);
                            //std::map<int, entry_t *> &m = DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_NEW_ORDER][r->get_key()];
                            //m[0] = dr->get_dep_entry(0);
                            //m[1] = dr->get_dep_entry(1);
                            //m[2] = dr->get_dep_entry(2);

                            tbl->remove(r, false); // don't release the row
                        }
                    } else { // deptran finish
                        auto &m_buf = (*row_map)[TPCC_TB_NEW_ORDER];
                        for (auto &it : m_buf) {
                            //verify(1 == DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_NEW_ORDER].erase(it.second->get_key())); // FIXME remove verify after debug
                            it.second->release();
                        }
                    }
                } else { // non deptran
                    if (r) {
                        if (!txn->remove_row(tbl, r)) {
                            *res = REJECT;
                            *output_size = output_index;
                            return;
                        }
                    }
                }

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_DELIVERY, piece: %d end", TPCC_DELIVERY_0);

            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_DELIVERY,
//            TPCC_DELIVERY_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//                Value buf;
//                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
//                mdb::MultiBlob mbl(3), mbh(3);
//                mbl[0] = input[1].get_blob();
//                mbh[0] = input[1].get_blob();
//                mbl[1] = input[0].get_blob();
//                mbh[1] = input[0].get_blob();
//                Value no_o_id_low(std::numeric_limits<i32>::min()), no_o_id_high(std::numeric_limits<i32>::max());
//                mbl[2] = no_o_id_low.get_blob();
//                mbh[2] = no_o_id_high.get_blob();
//                mdb::Table *tbl = txn->get_table(TPCC_TB_NEW_ORDER);
//
//                mdb::ResultSet rs = txn->query_in(tbl, mbl, mbh, mdb::ORD_ASC);
//                cell_locator_t cl(TPCC_TB_NEW_ORDER, 3);
//                cl.primary_key[0] = input[1].get_blob();
//                cl.primary_key[1] = input[0].get_blob();
//                if (rs.has_next()) {
//                    mdb::Row *r = rs.next();
//                    cl.primary_key[2] = r->get_blob(2);
//                    cl.col_id = 0;
//                    (*opset)[cl] = OP_IR | OP_W;
//                    cl.col_id = 1;
//                    (*opset)[cl] = OP_IR | OP_W;
//                    cl.col_id = 2;
//                    (*opset)[cl] = OP_IR | OP_W;
//                }
//                else { // no new order
//                }
//            });

    TxnRegistry::reg(
            TPCC_DELIVERY,
            TPCC_DELIVERY_1, // Ri & W order
            DF_NO,
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_1);
                verify(row_map == NULL);
                verify(input_size == 4);
                i32 output_index = 0;
                Value buf;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::MultiBlob mb(3);
                //cell_locator_t cl(TPCC_TB_ORDER, 3);
                mb[0] = input[2].get_blob();
                mb[1] = input[1].get_blob();
                mb[2] = input[0].get_blob();
                //Log::debug("Delivery: o_d_id: %d, o_w_id: %d, o_id: %d, hash: %u", input[2].get_i32(), input[1].get_i32(), input[0].get_i32(), mdb::MultiBlob::hash()(cl.primary_key));

                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_ORDER), mb,
                    output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 3, ALock::RLOCK),
                            mdb::column_lock_t(r, 5, ALock::WLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 3, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 5, succ_callback,
                    //    fail_callback);
                    return;
                }


                if (pv) {
                    ((DepRow *)r)->get_dep_entry(5)->touch(tv, true);
                }
                if (!txn->read_column(r, 3, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // read o_c_id

                if (!txn->write_column(r, 5, input[3])) { // write o_carrier_id
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_DELIVERY, piece: %d end", TPCC_DELIVERY_1);
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_DELIVERY,
//            TPCC_DELIVERY_1,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            cell_locator_t cl(TPCC_TB_ORDER, 3);
//            cl.primary_key[0] = input[2].get_blob();
//            cl.primary_key[1] = input[1].get_blob();
//            cl.primary_key[2] = input[0].get_blob();
//            //Log::debug("DeliveryLock: o_d_id: %d, o_w_id: %d, o_id: %d, hash: %u", input[2].get_i32(), input[1].get_i32(), input[0].get_i32(), mdb::MultiBlob::hash()(cl.primary_key));
//            //cl.col_id = 3;
//            //(*opset)[cl] = OP_IR;
//            cl.col_id = 5;
//            (*opset)[cl] = OP_W;
//            });

    TxnRegistry::reg(
            TPCC_DELIVERY,
            TPCC_DELIVERY_2, // Ri & W order_line
            DF_NO,
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_2);
                verify(row_map == NULL);
                verify(input_size == 3);
                i32 output_index = 0;
                Value buf;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::MultiBlob mbl(4), mbh(4);
                mbl[0] = input[2].get_blob();
                mbh[0] = input[2].get_blob();
                mbl[1] = input[1].get_blob();
                mbh[1] = input[1].get_blob();
                mbl[2] = input[0].get_blob();
                mbh[2] = input[0].get_blob();
                Value ol_number_low(std::numeric_limits<i32>::min()),
                ol_number_high(std::numeric_limits<i32>::max());
                mbl[3] = ol_number_low.get_blob();
                mbh[3] = ol_number_high.get_blob();

                mdb::ResultSet rs = txn->query_in(
                        txn->get_table(TPCC_TB_ORDER_LINE), mbl, mbh,
                        output_size, header.pid, mdb::ORD_ASC);
                double ol_amount_buf = 0.0;
                mdb::Row *r = NULL;
//                cell_locator_t cl(TPCC_TB_ORDER_LINE, 4);
//                cl.primary_key[0] = input[2].get_blob();
//                cl.primary_key[1] = input[1].get_blob();
//                cl.primary_key[2] = input[0].get_blob();

                std::vector<mdb::Row *> row_list;
                row_list.reserve(15);
                while (rs.has_next()) {
                    row_list.push_back(rs.next());
                }

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res,
                                ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    std::vector<mdb::column_lock_t> column_locks;
                    column_locks.reserve(2 * row_list.size());

                    int i = 0;
                    while (i < row_list.size()) {
                        r = row_list[i++];

                        column_locks.push_back(
                                mdb::column_lock_t(r, 6, ALock::WLOCK)
                                );
                        column_locks.push_back(
                                mdb::column_lock_t(r, 8, ALock::RLOCK)
                                );

                        //((mdb::Txn2PL *)txn)->reg_write_column(r, 6, succ_callback,
                        //    fail_callback);
                        //((mdb::Txn2PL *)txn)->reg_read_column(r, 8, succ_callback,
                        //    fail_callback);
                    }
                    ps->reg_rw_lock(column_locks, succ_callback, fail_callback);

                    return;
                }

                int i = 0;
                while (i < row_list.size()) {
                    r = row_list[i++];

                    if (pv) {
                        //cl.primary_key[3] = r->get_blob(3);
                        //cl.col_id = 6;
                        ((DepRow *)r)->get_dep_entry(6)->touch(tv, true);
                        ((DepRow *)r)->get_dep_entry(8)->touch(tv, true);
                    }

                    if (!txn->read_column(r, 8, &buf)) { // read ol_amount
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    ol_amount_buf += buf.get_double();
                    if (!txn->write_column(r, 6, Value(std::to_string(time(NULL))))) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                }
                output[output_index++] = Value(ol_amount_buf);

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
                Log::debug("TPCC_DELIVERY, piece: %d end", TPCC_DELIVERY_2);
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_DELIVERY,
//            TPCC_DELIVERY_2,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
//            mdb::MultiBlob mbl(4), mbh(4);
//            mbl[0] = input[2].get_blob();
//            mbh[0] = input[2].get_blob();
//            mbl[1] = input[1].get_blob();
//            mbh[1] = input[1].get_blob();
//            mbl[2] = input[0].get_blob();
//            mbh[2] = input[0].get_blob();
//            Value ol_number_low(std::numeric_limits<i32>::min()), ol_number_high(std::numeric_limits<i32>::max());
//            mbl[3] = ol_number_low.get_blob();
//            mbh[3] = ol_number_high.get_blob();
//
//            mdb::ResultSet rs = txn->query_in(txn->get_table(TPCC_TB_ORDER_LINE), mbl, mbh, mdb::ORD_ASC);
//
//            cell_locator_t cl(TPCC_TB_ORDER_LINE, 4);
//            cl.primary_key[0] = input[2].get_blob();
//            cl.primary_key[1] = input[1].get_blob();
//            cl.primary_key[2] = input[0].get_blob();
//
//            while (rs.has_next()) {
//                DepRow *r = (DepRow *)rs.next();
//                cl.primary_key[3] = r->get_blob(3);
//                cl.col_id = 6; // ol_delivery_d
//                (*opset)[cl] = OP_W;
//                cl.col_id = 8; // ol_amount
//                (*opset)[cl] = OP_IR;
//            }
//
//            });

    TxnRegistry::reg(
            TPCC_DELIVERY,
            TPCC_DELIVERY_3, // W customer
            DF_REAL,
            [] (const RequestHeader &header, const Value *input, i32 input_size, i32 *res, Value *output, i32 *output_size, row_map_t *row_map, Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, std::vector<TxnInfo *> *conflict_txns) {
                Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_3);
                verify(input_size == 4);
                i32 output_index = 0;
                Value buf;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::Row *r = NULL;
                mdb::MultiBlob mb(3);
                //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
                mb[0] = input[0].get_blob();
                mb[1] = input[2].get_blob();
                mb[2] = input[1].get_blob();

                if (row_map == NULL || pv != NULL) { // non deptran || deptran start req
                    r = txn->query(txn->get_table(TPCC_TB_CUSTOMER), mb,
                        output_size, header.pid).next();
                }

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 16, ALock::WLOCK),
                            mdb::column_lock_t(r, 19, ALock::WLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 16, succ_callback,
                    //    fail_callback);
                    //((mdb::Txn2PL *)txn)->reg_write_column(r, 19, succ_callback,
                    //    fail_callback);
                    return;
                }

                bool do_finish = true;
                if (row_map) { // deptran
                    if (pv) { // start req
                        (*row_map)[TPCC_TB_CUSTOMER][mb] = r;
                        DepRow *dr = (DepRow *)r;
                        dr->get_dep_entry(16)->touch(tv, false);
                        dr->get_dep_entry(19)->touch(tv, false);
                        do_finish = false;
                    } else {
                        //std::unordered_map<mdb::MultiBlob, mdb::Row *, mdb::MultiBlob::hash>::iterator it = (*row_map)[TPCC_TB_CUSTOMER].find(cl.primary_key);
                        //verify(it != (*row_map)[TPCC_TB_CUSTOMER].end()); //FIXME remove this line after debug
                        //r = (*row_map)[TPCC_TB_CUSTOMER][cl.primary_key];
                        r = row_map->begin()->second.begin()->second;
                        verify(r != NULL); //FIXME remove this line after debug
                        do_finish = true;
                    }
                }

                if (do_finish) {
                    if (!txn->read_column(r, 16, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    buf.set_double(buf.get_double() + input[3].get_double());
                    if (!txn->write_column(r, 16, buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    if (!txn->read_column(r, 19, &buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }
                    buf.set_i32(buf.get_i32() + (i32)1);
                    if (!txn->write_column(r, 19, buf)) {
                        *res = REJECT;
                        *output_size = output_index;
                        return;
                    }

                    verify(*output_size >= output_index);
                    *output_size = output_index;
                    *res = SUCCESS;
                    Log::debug("TPCC_DELIVERY, piece: %d end", TPCC_DELIVERY_3);
                }
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_DELIVERY,
//            TPCC_DELIVERY_3,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//            cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
//            cl.primary_key[0] = input[0].get_blob();
//            cl.primary_key[1] = input[2].get_blob();
//            cl.primary_key[2] = input[1].get_blob();
//            cl.col_id = 16; // c_balance
//            (*opset)[cl] = OP_W;
//            cl.col_id = 19; // c_delivery_cnt
//            (*opset)[cl] = OP_W;
//            });

}

void TpccPiece::reg_stock_level() {
    TxnRegistry::reg(
            TPCC_STOCK_LEVEL,
            TPCC_STOCK_LEVEL_0, // Ri district
            DF_NO,
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                verify(row_map == NULL);
                verify(input_size == 2);
                i32 output_index = 0;
                Value buf;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::MultiBlob mb(2);
                //cell_locator_t cl(TPCC_TB_DISTRICT, 2);
                mb[0] = input[1].get_blob();
                mb[1] = input[0].get_blob();

                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_DISTRICT), mb,
                    output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 10, ALock::RLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 10, succ_callback,
                    //    fail_callback);
                    return;
                }

                if (conflict_txns) {
                    ((DepRow *)r)->get_dep_entry(10)->ro_touch(conflict_txns);
                }
                if (!txn->read_column(r, 10, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                output[output_index++] = buf; // output[0] ==> d_next_o_id

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_STOCK_LEVEL,
//            TPCC_STOCK_LEVEL_0,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//                cell_locator_t cl(TPCC_TB_DISTRICT, 2);
//                cl.primary_key[0] = input[1].get_blob();
//                cl.primary_key[1] = input[0].get_blob();
//                cl.col_id = 10; // d_next_o_id
//                (*opset)[cl] = OP_IR;
//            });

    TxnRegistry::reg(TPCC_STOCK_LEVEL,
            TPCC_STOCK_LEVEL_1, // Ri order_line
            DF_NO,
            [] (const RequestHeader &header, const Value *input,
            i32 input_size, i32 *res, Value *output,
            i32 *output_size, row_map_t *row_map,
            Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv,
            std::vector<TxnInfo *> *conflict_txns) {

            verify(row_map == NULL);
            verify(input_size == 3);
            i32 output_index = 0;
            Value buf;
            mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
            mdb::MultiBlob mbl(4), mbh(4);
            mbl[0] = input[2].get_blob();
            mbh[0] = input[2].get_blob();
            mbl[1] = input[1].get_blob();
            mbh[1] = input[1].get_blob();
            Value ol_o_id_low(input[0].get_i32() - (i32)21);
            mbl[2] = ol_o_id_low.get_blob();
            mbh[2] = input[0].get_blob();
            Value ol_number_low(std::numeric_limits<i32>::max()),
                ol_number_high(std::numeric_limits<i32>::min());
            mbl[3] = ol_number_low.get_blob();
            mbh[3] = ol_number_high.get_blob();

            mdb::ResultSet rs = txn->query_in(
                    txn->get_table(TPCC_TB_ORDER_LINE), mbl, mbh, output_size,
                    header.pid, mdb::ORD_ASC);

            Log::debug("tid: %llx, stock_level: piece 1: d_next_o_id: %d, ol_w_id: %d, ol_d_id: %d", header.tid, input[0].get_i32(), input[1].get_i32(), input[2].get_i32());

            std::vector<mdb::Row *> row_list;
            row_list.reserve(20);

            while (rs.has_next()) {
                row_list.push_back(rs.next());
            }

            verify(row_list.size() != 0);

            if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                && output_size == NULL) {
                mdb::Txn2PL::PieceStatus *ps
                    = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                std::vector<mdb::column_lock_t> column_locks;
                column_locks.reserve(row_list.size());

                int i = 0;
                while (i < row_list.size()) {
                    column_locks.push_back(
                            mdb::column_lock_t(row_list[i++], 4, ALock::RLOCK)
                            );

                    //((mdb::Txn2PL *)txn)->reg_read_column(row_list[i++], 4,
                    //    succ_callback, fail_callback);
                }
                ps->reg_rw_lock(column_locks, succ_callback, fail_callback);

                return;
            }

            int i = 0;
            while (i < row_list.size()) {
                mdb::Row *r = row_list[i++];
                if (!txn->read_column(r, 4, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }

                //std::vector<Value> kkk;
                //txn->read_columns(r, std::vector<mdb::column_id_t>({
                //    0, 1, 2, 3, 4, 5}), &kkk);
                //Log::debug("ol_d_id: %d, ol_w_id: %d, ol_o_id: %d,"
                //      " ol_number: %d, ol_i_id: %d, ol_supply_w_id: %d",
                //      kkk[0].get_i32(), kkk[1].get_i32(), kkk[2].get_i32(),
                //      kkk[3].get_i32(), kkk[4].get_i32(), kkk[5].get_i32());
                output[output_index++] = buf;
            }

            verify(*output_size >= output_index);
            verify(*output_size >= 100);
            verify(*output_size <= 300);
            *output_size = output_index;
            *res = SUCCESS;
            });

    //    TxnRegistry::reg_lock_oracle(
    //            TPCC_STOCK_LEVEL,
    //            TPCC_STOCK_LEVEL_1,
    //            [] (const RequestHeader& header,
    //                const Value *input,
    //                rrr::i32 input_size,
    //                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
    //                ) {
    //            });

    TxnRegistry::reg(
            TPCC_STOCK_LEVEL,
            TPCC_STOCK_LEVEL_2, // R stock
            DF_NO,
            [] (const RequestHeader &header, 
                const Value *input, 
                i32 input_size, 
                i32 *res, 
                Value *output, 
                i32 *output_size, 
                row_map_t *row_map, 
                Vertex<PieInfo> *pv, 
                Vertex<TxnInfo> *tv, 
                std::vector<TxnInfo *> *conflict_txns) {

                verify(row_map == NULL);
                verify(input_size == 3);
                i32 output_index = 0;
                Value buf;
                mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                mdb::MultiBlob mb(2);
                //cell_locator_t cl(TPCC_TB_STOCK, 2);
                mb[0] = input[0].get_blob();
                mb[1] = input[1].get_blob();

                mdb::Row *r = txn->query(txn->get_table(TPCC_TB_STOCK), mb,
                    output_size, header.pid).next();

                if (DTxnMgr::get_sole_mgr()->get_mode() == MODE_2PL
                    && output_size == NULL) {
                    mdb::Txn2PL::PieceStatus *ps
                        = ((mdb::Txn2PL *)txn)->get_piece_status(header.pid);
                    std::function<void(void)> succ_callback = TPL::get_2pl_succ_callback(header, input, input_size, res, ps);
                    std::function<void(void)> fail_callback = TPL::get_2pl_fail_callback(header, res, ps);

                    ps->reg_rw_lock(
                        std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r, 2, ALock::RLOCK)
                            }), succ_callback, fail_callback);

                    //((mdb::Txn2PL *)txn)->reg_read_column(r, 2, succ_callback,
                    //    fail_callback);
                    return;
                }

                if (pv) {
                    ((DepRow *)r)->get_dep_entry(2)->ro_touch(conflict_txns);
                }

                if (!txn->read_column(r, 2, &buf)) {
                    *res = REJECT;
                    *output_size = output_index;
                    return;
                }
                if (buf.get_i32() < input[2].get_i32())
                    output[output_index++] = Value((i32)1);
                else
                    output[output_index++] = Value((i32)0);

                verify(*output_size >= output_index);
                *output_size = output_index;
                *res = SUCCESS;
            });

//    TxnRegistry::reg_lock_oracle(
//            TPCC_STOCK_LEVEL,
//            TPCC_STOCK_LEVEL_2,
//            [] (const RequestHeader& header,
//                const Value *input,
//                rrr::i32 input_size,
//                std::unordered_map<cell_locator_t, int, cell_locator_t_hash>* opset
//                ) {
//                cell_locator_t cl(TPCC_TB_STOCK, 2);
//                cl.primary_key[0] = input[0].get_blob();
//                cl.primary_key[1] = input[1].get_blob();
//
//                cl.col_id = 2; // s_quantity
//                (*opset)[cl] = OP_IR;
//            });

}

void TpccPiece::reg_all() {
    reg_new_order();
    reg_payment();
    reg_order_status();
    reg_delivery();
    reg_stock_level();
}

TpccPiece::TpccPiece() {}

TpccPiece::~TpccPiece() {}

}
