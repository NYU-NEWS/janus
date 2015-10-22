#include "all.h"

#include "bench/tpcc_real_dist/sharding.h"

namespace rococo {

void TpccPiece::reg_order_status() {
    BEGIN_PIE(TPCC_ORDER_STATUS, // RO
            TPCC_ORDER_STATUS_0, // piece 0, R customer secondary index, c_last -> c_id
            DF_NO) {
        // #################################################################
        verify(row_map == NULL);
        verify(input_size == 3);
        Log::debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_0);
        // #################################################################

        TPL_KISS_NONE;

        mdb::MultiBlob mbl(3), mbh(3);
        mbl[0] = input[2].get_blob();
        mbh[0] = input[2].get_blob();
        mbl[1] = input[1].get_blob();
        mbh[1] = input[1].get_blob();
        Value c_id_low(std::numeric_limits<i32>::min());
        Value c_id_high(std::numeric_limits<i32>::max());

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
            if (mid_set) if (inc) {
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
        Log::debug("w_id: %d, d_id: %d, c_last: %s, num customer: %d", 
                input[1].get_i32(), input[2].get_i32(), 
                input[0].get_str().c_str(), n_c);
        verify(mid_set);
        i32 oi = 0;
        output[oi++] = Value(it_mid->second);

        // #################################################################
        Log::debug("TPCC_ORDER_STATUS, piece: %d, end", TPCC_ORDER_STATUS_0);
        verify(*output_size >= oi);
        *res = SUCCESS;
        // #################################################################
        *output_size = oi;
    } END_PIE

    BEGIN_PIE(TPCC_ORDER_STATUS, // RO
            TPCC_ORDER_STATUS_1, // Ri customer
            DF_NO) {
        // #################################################################
        verify(row_map == NULL);
        verify(dtxn != nullptr);
        Log::debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_1);
        verify(input_size == 3);
        // #################################################################
        
        mdb::Table *tbl = dtxn->get_table(TPCC_TB_CUSTOMER);
        // R customer
        Value buf;
        mdb::MultiBlob mb(3);
        //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
        mb[0] = input[2].get_blob();
        mb[1] = input[1].get_blob();
        mb[2] = input[0].get_blob();
        mdb::Row *r = dtxn->query(tbl, mb, output_size, header.pid).next();

        // #################################################################
        TPL_KISS(
                mdb::column_lock_t(r, 3, ALock::RLOCK),
                mdb::column_lock_t(r, 4, ALock::RLOCK),
                mdb::column_lock_t(r, 5, ALock::RLOCK),
                mdb::column_lock_t(r, 16, ALock::RLOCK)
        )
        // #################################################################

        if ((IS_MODE_RCC || IS_MODE_RO6)) {
            ((RCCDTxn *) dtxn)->kiss(r, 16, false);
        }

        if (RO6_RO_PHASE_1) return;

        i32 oi = 0; 
        dtxn->read_column(r, 3, &output[oi++]);// read c_first
        dtxn->read_column(r, 4, &output[oi++]);// read c_middle
        dtxn->read_column(r, 5, &output[oi++]);// read c_last
        dtxn->read_column(r, 16, &output[oi++]);// read c_balance

        // #################################################################
        verify(*output_size >= oi);
        *output_size = oi;
        *res = SUCCESS;
        Log::debug("TPCC_ORDER_STATUS, piece: %d, end", TPCC_ORDER_STATUS_1);
        // #################################################################
    } END_PIE

    BEGIN_PIE(TPCC_ORDER_STATUS, // RO
            TPCC_ORDER_STATUS_2, // Ri order
            DF_NO) {
        verify(row_map == NULL);
        Log::debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_2);
        verify(input_size == 3);

        mdb::MultiBlob mb_0(3);
        mb_0[0] = input[1].get_blob();
        mb_0[1] = input[0].get_blob();
        mb_0[2] = input[2].get_blob();
        mdb::Row *r_0 = dtxn->query(
                dtxn->get_table(TPCC_TB_ORDER_C_ID_SECONDARY), mb_0,
                output_size, header.pid).next();

        mdb::MultiBlob mb(3);
        mb[0] = input[1].get_blob();
        mb[1] = input[0].get_blob();
        mb[2] = r_0->get_blob(3); // FIXME add lock before reading 

        mdb::Row *r = dtxn->query(
                dtxn->get_table(TPCC_TB_ORDER), mb,
                false, header.pid).next();


        if (IS_MODE_2PL && output_size == NULL) {
            mdb::Txn2PL::PieceStatus *ps
                    = ((mdb::Txn2PL *) dtxn)->get_piece_status(header.pid);

//            std::function<void(
//                    const RequestHeader &,
//                    const Value *,
//                    rrr::i32,
//                    rrr::i32 *)> func =
//                    [r_0, dtxn, ps](
//                            const RequestHeader &header,
//                            const Value *input,
//                            rrr::i32 input_size,
//                            rrr::i32 *res) {
//                        //dtxn->read_column(r_0, 3, &buf);
//                        mdb::MultiBlob mb(3);
//                        mb[0] = input[1].get_blob();
//                        mb[1] = input[0].get_blob();
//                        mb[2] = r_0->get_blob(3);
//
//                        mdb::Row *r = dtxn->query(
//                                dtxn->get_table(TPCC_TB_ORDER), mb,
//                                false, header.pid).next();
//
//                        std::function<void(void)> succ_callback1 = 
//                            ((TPLDTxn *) dtxn)->get_2pl_succ_callback(
//                                header, input, input_size, res, ps);
//
//                        std::function<void(void)> fail_callback1 = 
//                            ((TPLDTxn *) dtxn)->get_2pl_fail_callback(
//                                        header, res, ps);
//
//                        ps->set_num_waiting_locks(1);
//
//                        Log::debug("PS1: %p", ps);
//
//                        ps->reg_rw_lock(
//                                std::vector<mdb::column_lock_t>({
//                                        mdb::column_lock_t(r, 2, ALock::RLOCK),
//                                        mdb::column_lock_t(r, 4, ALock::RLOCK),
//                                        mdb::column_lock_t(r, 5, ALock::RLOCK)
//                                }), succ_callback1, fail_callback1);
//                        return;
//                    };

            std::function<void(void)> succ_callback = 
                ((TPLDTxn *) dtxn)->get_2pl_succ_callback(
                    header, input, input_size, res, ps);

            std::function<void(void)> fail_callback = 
                ((TPLDTxn *) dtxn)->get_2pl_fail_callback(
                    header, res, ps);

            ps->reg_rw_lock(
                    std::vector<mdb::column_lock_t>({
                            mdb::column_lock_t(r_0, 3, ALock::RLOCK),
                            mdb::column_lock_t(r, 2, ALock::RLOCK),
                            mdb::column_lock_t(r, 4, ALock::RLOCK),
                            mdb::column_lock_t(r, 5, ALock::RLOCK)
                    }), succ_callback, fail_callback);

            //((mdb::Txn2PL *)txn)->reg_read_column(r_0, 3, succ_callback,
            //    fail_callback);
            return;
        }

//        dtxn->read_column(r_0, 3, &buf);

        //mdb::MultiBlob mb(3);
        ////cell_locator_t cl(TPCC_TB_ORDER, 3);
        //mb[0] = input[1].get_blob();
        //mb[1] = input[0].get_blob();
        //mb[2] = r_0->get_blob(3);

//        mdb::Row *r = dtxn->query(dtxn->get_table(TPCC_TB_ORDER), mb,
//                true, header.pid).next();

        if ((IS_MODE_RCC || IS_MODE_RO6)) {
            ((RCCDTxn *) dtxn)->kiss(r, 5, false);
        }

        if (RO6_RO_PHASE_1) return;

        i32 oi = 0;
        dtxn->read_column(r, 2, &output[oi++]); // output[0] ==> o_id
        dtxn->read_column(r, 4, &output[oi++]); // output[1] ==> o_entry_d
        dtxn->read_column(r, 5, &output[oi++]); // output[2] ==> o_carrier_id
        Log::debug("piece: %d, o_id: %d", TPCC_ORDER_STATUS_2, output[0].get_i32());

        // ############################################################
        verify(*output_size >= oi);
        *res = SUCCESS;
        Log::debug("TPCC_ORDER_STATUS, piece: %d, end", TPCC_ORDER_STATUS_2);
        // ############################################################
        *output_size = oi;
    } END_PIE


    BEGIN_PIE(TPCC_ORDER_STATUS, // RO
            TPCC_ORDER_STATUS_3, // R order_line
            DF_NO) {
        verify(row_map == NULL);
        Log::debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_3);
        verify(input_size == 3);
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

        mdb::ResultSet rs = dtxn->query_in(
                dtxn->get_table(TPCC_TB_ORDER_LINE), mbl, mbh,
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

        if (IS_MODE_2PL && output_size == NULL) {
            mdb::Txn2PL::PieceStatus *ps = 
                ((mdb::Txn2PL *) dtxn)->get_piece_status(header.pid);
            std::function<void(void)> succ_callback = 
                ((TPLDTxn *) dtxn)->get_2pl_succ_callback(
                                            header, input, 
                                            input_size, res, ps);
            std::function<void(void)> fail_callback = 
                ((TPLDTxn *) dtxn)->get_2pl_fail_callback(header, res, ps);

            std::vector<mdb::column_lock_t> column_locks;
            column_locks.reserve(5 * row_list.size());

            int i = 0;
            Log::debug("row_list size: %u", row_list.size());
            while (i < row_list.size()) {
                r = row_list[i++];

            // ############################################################
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
            // ############################################################
            }

            ps->reg_rw_lock(column_locks, succ_callback, fail_callback);
            return;
        }

        if (IS_MODE_RCC || IS_MODE_RO6) {
            for (int i = 0; i < row_list.size(); i++) {
                r = row_list[i++];
                ((RCCDTxn*)dtxn)->kiss(r, 6, false);
            }
            if (RO6_RO_PHASE_1) return;
        }

        int i = 0;
        i32 oi = 0;
        while (i < row_list.size()) {
            r = row_list[i++];
            dtxn->read_column(r, 4, &output[oi++]); // output[0] ==> ol_i_id
            dtxn->read_column(r, 5, &output[oi++]); // output[1] ==> ol_supply_w_id
            dtxn->read_column(r, 6, &output[oi++]); // output[2] ==> ol_delivery_d
            dtxn->read_column(r, 7, &output[oi++]); // output[3] ==> ol_quantity
            dtxn->read_column(r, 8, &output[oi++]); // output[4] ==> ol_amount
        }

        // ############################################################
        verify(*output_size >= oi);
        *output_size = oi;
        *res = SUCCESS;
        Log::debug("TPCC_ORDER_STATUS, piece: %d, end", TPCC_ORDER_STATUS_3);
        // ############################################################
    } END_PIE
}
} // namespace rococo
