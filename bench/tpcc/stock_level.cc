#include "all.h"

namespace rococo {

void TpccPiece::reg_stock_level() {
    BEGIN_PIE(TPCC_STOCK_LEVEL,
            TPCC_STOCK_LEVEL_0, // Ri district
            DF_NO) {
        // ###################################################
        verify(row_map == NULL);
        verify(dtxn != nullptr);
        verify(input_size == 2);
        // ###################################################
        mdb::MultiBlob mb(2);
        //cell_locator_t cl(TPCC_TB_DISTRICT, 2);
        mb[0] = input[1].get_blob();
        mb[1] = input[0].get_blob();

        mdb::Row *r = dtxn->query(dtxn->get_table(TPCC_TB_DISTRICT), mb,
                output_size, header.pid).next();

        TPL_KISS(mdb::column_lock_t(r, 10, ALock::RLOCK));

        if ((IS_MODE_RCC || IS_MODE_RO6)) {
            ((RCCDTxn *) dtxn)->kiss(r, 10, false);
        }

        if (RO6_RO_PHASE_1) return;

        i32 oi = 0;
        dtxn->read_column(r, 10, &output[oi++]); // output[0] ==> d_next_o_id

        // ###################################################
        verify(*output_size >= oi);
        *output_size = oi;
        *res = SUCCESS;
        // ###################################################
    } END_PIE

    BEGIN_PIE(TPCC_STOCK_LEVEL,
            TPCC_STOCK_LEVEL_1, // Ri order_line
            DF_NO) {
        verify(row_map == NULL);
        verify(input_size == 3);

        mdb::MultiBlob mbl(4), mbh(4);
        mbl[0] = input[2].get_blob();
        mbh[0] = input[2].get_blob();
        mbl[1] = input[1].get_blob();
        mbh[1] = input[1].get_blob();
        Value ol_o_id_low(input[0].get_i32() - (i32) 21);
        mbl[2] = ol_o_id_low.get_blob();
        mbh[2] = input[0].get_blob();
        Value ol_number_low(std::numeric_limits<i32>::max()),
                ol_number_high(std::numeric_limits<i32>::min());
        mbl[3] = ol_number_low.get_blob();
        mbh[3] = ol_number_high.get_blob();

        mdb::ResultSet rs = dtxn->query_in(
                dtxn->get_table(TPCC_TB_ORDER_LINE), mbl, mbh, output_size,
                header.pid, mdb::ORD_ASC);

        Log_debug("tid: %llx, stock_level: piece 1: d_next_o_id: %d, ol_w_id: %d, ol_d_id: %d",
                header.tid, input[0].get_i32(), input[1].get_i32(), input[2].get_i32());

        std::vector<mdb::Row *> row_list;
        row_list.reserve(20);

        while (rs.has_next()) {
            row_list.push_back(rs.next());
        }

        verify(row_list.size() != 0);

        if (IS_MODE_2PL && output_size == NULL) {
            auto mdb_txn = (mdb::Txn2PL*) dtxn->mdb_txn_;
            auto ps = mdb_txn->get_piece_status(header.pid);

            std::function<void(void)> succ_callback = 
                ((TPLDTxn *) dtxn)->get_2pl_succ_callback(
                    header, input, input_size, res, ps);

            std::function<void(void)> fail_callback = 
                ((TPLDTxn *) dtxn)->get_2pl_fail_callback(
                    header, res, ps);

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

        i32 i = 0;
        i32 oi = 0;
        while (i < row_list.size()) {
            mdb::Row *r = row_list[i++];
            dtxn->read_column(r, 4, &output[oi++]);
        }

        // ###################################################
        verify(*output_size >= oi);
        verify(*output_size >= 100);
        verify(*output_size <= 300);
        *res = SUCCESS;
        // ###################################################
        *output_size = oi;
    } END_PIE

    BEGIN_PIE(TPCC_STOCK_LEVEL,
            TPCC_STOCK_LEVEL_2, // R stock
            DF_NO) {
        verify(row_map == NULL);
        verify(input_size == 3);
        i32 output_index = 0;
        Value buf;
        mdb::MultiBlob mb(2);
        //cell_locator_t cl(TPCC_TB_STOCK, 2);
        mb[0] = input[0].get_blob();
        mb[1] = input[1].get_blob();

        mdb::Row *r = dtxn->query(dtxn->get_table(TPCC_TB_STOCK), mb,
                output_size, header.pid).next();


        TPL_KISS(mdb::column_lock_t(r, 2, ALock::RLOCK));

        if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) {
            ((RCCDTxn *) dtxn)->kiss(r, 2, false);
        }

        if (RO6_RO_PHASE_1) return;

        dtxn->read_column(r, 2, &buf);

        if (buf.get_i32() < input[2].get_i32())
            output[output_index++] = Value((i32) 1);
        else
            output[output_index++] = Value((i32) 0);

        // ##############################################
        verify(*output_size >= output_index);
        *output_size = output_index;
        *res = SUCCESS;
        // ##############################################
    } END_PIE
}

} // namespace rococo
