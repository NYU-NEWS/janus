#include "all.h"

namespace rococo {

void TpccPiece::reg_new_order() {

    BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_0, // Ri & W district
            DF_NO) {
        verify(row_map == NULL);
        verify(input_size == 2);

        i32 output_index = 0;
        Value buf;
        mdb::MultiBlob mb(2);
        mb[0] = input[1].get_blob();
        mb[1] = input[0].get_blob();
        mdb::Row *r = dtxn->query(dtxn->get_table(TPCC_TB_DISTRICT), mb,
                output_size, header.pid).next();

        TPL_KISS(
                mdb::column_lock_t(r, 8, ALock::RLOCK),
                mdb::column_lock_t(r, 10, ALock::WLOCK)
        );

        if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) {
            ((RCCDTxn*)dtxn)->kiss(r, 10, true);
        }

        // R district
        if (!dtxn->read_column(r, 8, &buf)) { // read d_tax
            *res = REJECT;
            *output_size = output_index;
            return;
        }
        output[output_index++] = buf;
        if (!dtxn->read_column(r, 10, &buf)) { // read d_next_o_id
            *res = REJECT;
            *output_size = output_index;
            return;
        }
        output[output_index++] = buf;

        // W district
        buf.set_i32((i32)(buf.get_i32() + 1));
        if (!dtxn->write_column(r, 10, buf)) { // read d_next_o_id, increament by 1
            *res = REJECT;
            *output_size = output_index;
            return;
        }

        verify(*output_size >= output_index);
        *output_size = output_index;
        *res = SUCCESS;
        Log::debug("TPCC_NEW_ORDER, piece: %d end", TPCC_NEW_ORDER_0);
        return;
    } END_PIE

    BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_1, // R warehouse
            DF_NO) {
        verify(input_size == 1);
        verify(row_map == NULL);
        Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_1);

        i32 output_index = 0;
        Value buf;
        mdb::Row *r = dtxn->query(dtxn->get_table(TPCC_TB_WAREHOUSE),
                input[0], output_size, header.pid).next();

        TPL_KISS(mdb::column_lock_t(r, 7, ALock::RLOCK));

        // R warehouse
        if (!dtxn->read_column(r, 7, &buf)) { // read w_tax
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

    BEGIN_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_2, // R customer
            DF_NO //XXX either i or d is ok
    ) {

        verify(row_map == NULL);
        verify(input_size == 3);
        Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_2);
        i32 output_index = 0;
        Value buf;
        mdb::MultiBlob mb(3);
        mb[0] = input[2].get_blob();
        mb[1] = input[1].get_blob();
        mb[2] = input[0].get_blob();
        mdb::Row *r = dtxn->query(dtxn->get_table(TPCC_TB_CUSTOMER), mb,
                output_size, header.pid).next();

        TPL_KISS(
                mdb::column_lock_t(r, 5, ALock::RLOCK),
                mdb::column_lock_t(r, 13, ALock::RLOCK),
                mdb::column_lock_t(r, 15, ALock::RLOCK)
        );


        // R customer
        if (!dtxn->read_column(r, 5, &buf)) {
            *res = REJECT;
            *output_size = output_index;
            return;
        }
        output[output_index++] = buf;

        if (!dtxn->read_column(r, 13, &buf)) {
            *res = REJECT;
            *output_size = output_index;
            return;
        }
        output[output_index++] = buf;

        if (!dtxn->read_column(r, 15, &buf)) {
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

    BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_3, // W order
            DF_REAL) {
        verify(input_size == 7);
        Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_3);
        i32 output_index = 0;
        mdb::Table *tbl = dtxn->get_table(TPCC_TB_ORDER);
        mdb::Row *r = NULL;

        if (IS_MODE_2PL && TPL_PHASE_1) {
            mdb::MultiBlob mb(3);
            mb[0] = input[1].get_blob();
            mb[1] = input[2].get_blob();
            mb[2] = input[3].get_blob();

            r = dtxn->query(dtxn->get_table(TPCC_TB_ORDER_C_ID_SECONDARY),
                    mb, false, header.pid).next();
            TPL_KISS(mdb::column_lock_t(r, 3, ALock::WLOCK));
        }

        // W order
        if (!(IS_MODE_RCC || IS_MODE_RO6) ||
                ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1)) { // non-rcc || rcc start request
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

            CREATE_ROW(tbl->schema(), row_data);
        }

        bool do_finish = true;
        if (row_map) { // rococo
            if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) { // start req
                //std::map<int, entry_t *> &m = DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER][r->get_key()];
                ((RCCDTxn*)dtxn)->kiss(r, 0, false);
                ((RCCDTxn*)dtxn)->kiss(r, 1, false);
                ((RCCDTxn*)dtxn)->kiss(r, 2, false);
                ((RCCDTxn*)dtxn)->kiss(r, 5, false);

                (*row_map)[TPCC_TB_ORDER][r->get_key()] = r;

                do_finish = false;
            } else { // finish req

                //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER].erase(mb);
                auto kv = row_map->begin()->second.begin();
                r = kv->second;
                //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER].erase(kv->first);
                verify(r != NULL); //FIXME remove this line after debug
            }
        }

        if (do_finish) { // not deptran
            if (!dtxn->insert_row(tbl, r)) {
                *res = REJECT;
                *output_size = output_index;
                return;
            }

            // write TPCC_TB_ORDER_C_ID_SECONDARY
            mdb::MultiBlob mb(3);
            mb[0] = input[1].get_blob();
            mb[1] = input[2].get_blob();
            mb[2] = input[3].get_blob();
            r = dtxn->query(dtxn->get_table(TPCC_TB_ORDER_C_ID_SECONDARY),
                    mb, true, header.pid).next();
            if (!dtxn->write_column(r, 3, input[0])) {
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
    } END_PIE


    BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_4, // W new_order
            DF_REAL) {

        verify(input_size == 3);
        Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_4);

        if (IS_MODE_2PL && output_size == NULL) {
            ((TPLDTxn*)dtxn)->get_2pl_proceed_callback(header, input,
                    input_size, res)();
            return;
        }

        i32 output_index = 0;
        mdb::Table *tbl = dtxn->get_table(TPCC_TB_NEW_ORDER);
        mdb::Row *r = NULL;

        // W new_order
        if (!(IS_MODE_RCC || IS_MODE_RO6) || ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1)) { // non-rcc || rcc start request
            std::vector<Value> row_data({
                    input[1],   // o_d_id
                    input[2],   // o_w_id
                    input[0],   // o_id
            });

            CREATE_ROW(tbl->schema(), row_data);
        }

        bool do_finish = true;
        if (row_map) { // deptran
            if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) { // start req
                //std::map<int, entry_t *> &m = DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_NEW_ORDER][r->get_key()];

                ((RCCDTxn*)dtxn)->kiss(r, 0, false);
                ((RCCDTxn*)dtxn)->kiss(r, 1, false);
                ((RCCDTxn*)dtxn)->kiss(r, 2, false);

                (*row_map)[TPCC_TB_NEW_ORDER][r->get_key()] = r;

                do_finish = false;
            } else {

                auto kv = row_map->begin()->second.begin();
                r = kv->second;
                //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_NEW_ORDER].erase(kv->first);
                verify(r != NULL); //FIXME remove this line after debug
            }
        }

        if (do_finish) {
            if (!dtxn->insert_row(tbl, r)) {
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
    } END_PIE


    BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_5, // Ri item
            DF_NO) {

        verify(row_map == NULL);
        verify(input_size == 1);
        Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_5);
        i32 output_index = 0;
        Value buf;
        mdb::Row *r = dtxn->query(dtxn->get_table(TPCC_TB_ITEM), input[0],
                output_size, header.pid).next();

        TPL_KISS(
                mdb::column_lock_t(r, 2, ALock::RLOCK),
                mdb::column_lock_t(r, 3, ALock::RLOCK),
                mdb::column_lock_t(r, 4, ALock::RLOCK)
        );

        // Ri item
        if (!dtxn->read_column(r, 2, &buf)) {
            *res = REJECT;
            *output_size = output_index;
            return;
        }
        output[output_index++] = buf; // 0 ==> i_name
        if (!dtxn->read_column(r, 3, &buf)) {
            *res = REJECT;
            *output_size = output_index;
            return;
        }
        output[output_index++] = buf; // 1 ==> i_price
        if (!dtxn->read_column(r, 4, &buf)) {
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
    } END_PIE

    BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_6, // Ri stock
            DF_NO) {
        verify(row_map == NULL);
        verify(input_size == 3);
        Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_6);
        i32 output_index = 0;
        Value buf;
        mdb::MultiBlob mb(2);
        mb[0] = input[0].get_blob();
        mb[1] = input[1].get_blob();
        mdb::Row *r = dtxn->query(dtxn->get_table(TPCC_TB_STOCK), mb,
                output_size, header.pid).next();

        TPL_KISS(
                mdb::column_lock_t(r, 3, ALock::RLOCK),
                mdb::column_lock_t(r, 16, ALock::RLOCK)
        );

        //i32 s_dist_col = 3 + input[2].get_i32();
        // Ri stock
        if (!dtxn->read_column(r, 3, &buf)) { // FIXME compress all s_dist_xx into one column
            *res = REJECT;
            *output_size = output_index;
            return;
        }
        output[output_index++] = buf; // 0 ==> s_dist_xx

        if (!dtxn->read_column(r, 16, &buf)) {
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
    } END_PIE


    BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_7, // W stock
            DF_REAL) {
        verify(input_size == 4);
        Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_7);
        i32 output_index = 0;
        Value buf;
        mdb::Row *r = NULL;
        mdb::MultiBlob mb(2);
        mb[0] = input[0].get_blob();
        mb[1] = input[1].get_blob();
        if (!(IS_MODE_RCC || IS_MODE_RO6) || ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1)) { // non-rcc || rcc start request
            r = dtxn->query(dtxn->get_table(TPCC_TB_STOCK), mb,
                    output_size, header.pid).next();
        }

        TPL_KISS(
                mdb::column_lock_t(r, 2, ALock::WLOCK),
                mdb::column_lock_t(r, 13, ALock::WLOCK),
                mdb::column_lock_t(r, 14, ALock::WLOCK),
                mdb::column_lock_t(r, 15, ALock::WLOCK)
        );

        bool do_finish = true;
        if (row_map) { // deptran
            if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) { // start req
                (*row_map)[TPCC_TB_STOCK][mb] = r;
                //Log::debug("stock P1: hv: %u, k: %u, r: %p", mdb::MultiBlob::hash()((*row_map)[TPCC_TB_STOCK].begin()->first), mdb::MultiBlob::hash()(cl.primary_key), r);

                ((RCCDTxn*)dtxn)->kiss(r, 2, false);
                ((RCCDTxn*)dtxn)->kiss(r, 13, false);
                ((RCCDTxn*)dtxn)->kiss(r, 14, false);
                ((RCCDTxn*)dtxn)->kiss(r, 15, false);


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
            if (!dtxn->read_column(r, 2, &buf)) {
                *res = REJECT;
                *output_size = output_index;
                return;
            }
            new_ol_quantity = buf.get_i32() - input[2].get_i32();

            if (!dtxn->read_column(r, 13, &buf)) {
                *res = REJECT;
                *output_size = output_index;
                return;
            }
            Value new_s_ytd(buf.get_i32() + input[2].get_i32());

            if (!dtxn->read_column(r, 14, &buf)) {
                *res = REJECT;
                *output_size = output_index;
                return;
            }
            Value new_s_order_cnt((i32)(buf.get_i32() + 1));

            if (!dtxn->read_column(r, 15, &buf)) {
                *res = REJECT;
                *output_size = output_index;
                return;
            }
            Value new_s_remote_cnt(buf.get_i32() + input[3].get_i32());

            if (new_ol_quantity < 10)
                new_ol_quantity += 91;
            Value new_ol_quantity_value(new_ol_quantity);

            if (!dtxn->write_columns(r, std::vector<mdb::column_id_t>({
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
    } END_PIE

    BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_8, // W order_line
            DF_REAL) {
        verify(input_size == 10);
        Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_8);

        if (IS_MODE_2PL
                && output_size == NULL) {
            ((TPLDTxn*)dtxn)->get_2pl_proceed_callback(header, input,
                    input_size, res)();
            return;
        }

        i32 output_index = 0;
        mdb::Table *tbl = dtxn->get_table(TPCC_TB_ORDER_LINE);
        mdb::Row *r = NULL;

        if (!(IS_MODE_RCC || IS_MODE_RO6) || ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1)) { // non-rcc || rcc start request
            std::vector<Value> input_buf(input, input + input_size);

            CREATE_ROW(tbl->schema(), input_buf);
        }

        bool do_finish = true;
        if (row_map) { // deptran
            if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) { // start req
                //std::map<int, entry_t *> &m = DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER_LINE][r->get_key()];

                ((RCCDTxn*)dtxn)->kiss(r, 0, false);
                ((RCCDTxn*)dtxn)->kiss(r, 1, false);
                ((RCCDTxn*)dtxn)->kiss(r, 2, false);
                ((RCCDTxn*)dtxn)->kiss(r, 3, false);
                ((RCCDTxn*)dtxn)->kiss(r, 4, false);
                ((RCCDTxn*)dtxn)->kiss(r, 6, false);
                ((RCCDTxn*)dtxn)->kiss(r, 8, false);


                (*row_map)[TPCC_TB_ORDER_LINE][r->get_key()] = r;

                do_finish = false;
            } else { // finish req

                auto kv = row_map->begin()->second.begin();
                r = kv->second;
                //DepTranServiceImpl::dep_s->rw_entries_[TPCC_TB_ORDER_LINE].erase(kv->first);
                verify(r != NULL); //FIXME remove this line after debug
            }
        }

        if (do_finish) {
            if (!dtxn->insert_row(tbl, r)) {
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
    } END_PIE

}

} // namespace rococo