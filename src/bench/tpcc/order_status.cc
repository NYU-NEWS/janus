#include "procedure.h"

namespace janus {

void TpccProcedure::OrderStatusInit(TxRequest &req) {
  order_status_dep_.piece_order = false;
  /**
   * req.input_
   *  0       ==> w_id
   *  1       ==> d_id
   *  2       ==> c_id or last_name
   **/

  n_pieces_all_ = 4;

  if (req.input_.count(TPCC_VAR_C_ID) > 0) { // query by c_id
    status_[TPCC_ORDER_STATUS_0] = OUTPUT_READY; // piece 0 not needed
    status_[TPCC_ORDER_STATUS_1] = DISPATCHABLE; // piece 1 ready
    status_[TPCC_ORDER_STATUS_2] = DISPATCHABLE; // piece 2 ready
    order_status_dep_.piece_last2id = true;
    order_status_dep_.piece_ori_last2id = true;

    n_pieces_dispatched_ = 1; // since piece 0 not needed, set it as one started piece
    n_pieces_dispatch_acked_ = 1;
    n_pieces_dispatchable_ = 3;
  } else { // query by c_last
    // piece 0, R customer, c_last --> c_id
    GetWorkspace(TPCC_ORDER_STATUS_0).keys_ = {
        TPCC_VAR_C_LAST,
        TPCC_VAR_W_ID,
        TPCC_VAR_D_ID
    };
    output_size_[TPCC_ORDER_STATUS_0] = 1; // return c_id only
    p_types_[TPCC_ORDER_STATUS_0] = TPCC_ORDER_STATUS_0;

    status_[TPCC_ORDER_STATUS_0] = DISPATCHABLE;  // piece 0 ready
    n_pieces_dispatchable_ = 1;

    order_status_dep_.piece_last2id = false;
    order_status_dep_.piece_ori_last2id = false;

    status_[TPCC_ORDER_STATUS_1] = WAITING; // piece 1 waiting for piece 0
    status_[TPCC_ORDER_STATUS_2] = WAITING; // piece 2 waiting for piece 0
  }

  GetWorkspace(TPCC_ORDER_STATUS_1).keys_ = {
      TPCC_VAR_W_ID,
      TPCC_VAR_D_ID,
      TPCC_VAR_C_ID
  };
  // piece 1, R customer, depends on piece 0 if using c_last instead of c_id
  output_size_[TPCC_ORDER_STATUS_1] = 4;
  p_types_[TPCC_ORDER_STATUS_1] = TPCC_ORDER_STATUS_1;

  // piece 2, R order, depends on piece 0 if using c_last instead of c_id
  GetWorkspace(TPCC_ORDER_STATUS_2).keys_ = {
      TPCC_VAR_W_ID,
      TPCC_VAR_D_ID,
      TPCC_VAR_C_ID
  };
  output_size_[TPCC_ORDER_STATUS_2] = 3;
  p_types_[TPCC_ORDER_STATUS_2] = TPCC_ORDER_STATUS_2;

  // piece 3, R order_line, depends on piece 2
  GetWorkspace(TPCC_ORDER_STATUS_3).keys_ = {
      TPCC_VAR_W_ID,
      TPCC_VAR_D_ID,
      TPCC_VAR_O_ID
  };
  output_size_[TPCC_ORDER_STATUS_3] = 15 * 5;
  p_types_[TPCC_ORDER_STATUS_3] = TPCC_ORDER_STATUS_3;
  status_[TPCC_ORDER_STATUS_3] = WAITING;

}

//
//void TpccTxn::order_status_shard(const char *tb,
//                                     map<int32_t, Value> &input,
//                                     uint32_t &site) {
//  MultiValue mv;
//  if (tb == TPCC_TB_CUSTOMER ||
//      tb == TPCC_TB_ORDER ||
//      tb == TPCC_TB_ORDER_LINE) {
//    mv = MultiValue(input[TPCC_VAR_W_ID]);
//  } else {
//    verify(0);
//  }
//  int ret = sss_->get_site_id_from_tb(tb, mv, site);
//  verify(ret == 0);
//}


void TpccProcedure::OrderStatusRetry() {
  order_status_dep_.piece_last2id = order_status_dep_.piece_ori_last2id;
  order_status_dep_.piece_order = false;

  if (order_status_dep_.piece_last2id) {
    status_[TPCC_ORDER_STATUS_0] = OUTPUT_READY;
    status_[TPCC_ORDER_STATUS_1] = DISPATCHABLE;
    status_[TPCC_ORDER_STATUS_2] = DISPATCHABLE;
    n_pieces_dispatched_ = 1;
    n_pieces_dispatch_acked_ = 1;
    n_pieces_dispatchable_ = 3;
  }
  else {
    status_[TPCC_ORDER_STATUS_0] = DISPATCHABLE;
    status_[TPCC_ORDER_STATUS_1] = WAITING;
    status_[TPCC_ORDER_STATUS_2] = WAITING;
    n_pieces_dispatchable_ = 1;
  }
  status_[TPCC_ORDER_STATUS_3] = WAITING;
}


void TpccWorkload::RegOrderStatus() {
  // piece 0, R customer secondary index, c_last -> c_id
  RegP(TPCC_ORDER_STATUS, TPCC_ORDER_STATUS_0,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_C_LAST}, // i
       {}, // o
       {}, // c
       {TPCC_TB_CUSTOMER, {TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
         verify(cmd.input.size() >= 3);
         Log_debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_0);

         mdb::MultiBlob mbl(3), mbh(3);
         mbl[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mbh[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mbl[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mbh[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         Value c_id_low(std::numeric_limits<i32>::min());
         Value c_id_high(std::numeric_limits<i32>::max());

         mbl[2] = c_id_low.get_blob();
         mbh[2] = c_id_high.get_blob();
         c_last_id_t key_low
             (cmd.input[TPCC_VAR_C_LAST].get_str(), mbl, &(C_LAST_SCHEMA));
         c_last_id_t key_high
             (cmd.input[TPCC_VAR_C_LAST].get_str(), mbh, &(C_LAST_SCHEMA));
         std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high,
             it_mid;
         bool inc = false, mid_set = false;
         it_low = C_LAST2ID.lower_bound(key_low);
         it_high = C_LAST2ID.upper_bound(key_high);
         int n_c = 0;
         for (it = it_low; it != it_high; it++) {
           n_c++;
           if (mid_set)
             if (inc) {
               it_mid++;
               inc = false;
             } else
               inc = true;
           else {
             mid_set = true;
             it_mid = it;
           }
         }
         Log_debug("w_id: %d, d_id: %d, c_last: %s, num customer: %d",
                   cmd.input[TPCC_VAR_W_ID].get_i32(),
                   cmd.input[TPCC_VAR_D_ID].get_i32(),
                   cmd.input[TPCC_VAR_C_LAST].get_str().c_str(),
                   n_c);
         verify(mid_set);
         i32 oi = 0;
         output[TPCC_VAR_C_ID] = Value(it_mid->second);

         *res = SUCCESS;
       }
  );

  // Ri customer
  RegP(TPCC_ORDER_STATUS, TPCC_ORDER_STATUS_1,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_C_ID}, // i
       {}, // o
       {}, // c
       {TPCC_TB_CUSTOMER, {TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
         Log_debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_1);
         verify(cmd.input.size() >= 3);

         mdb::Table *tbl = tx.GetTable(TPCC_TB_CUSTOMER);
         // R customer
         Value buf;
         mdb::MultiBlob mb(3);
         mb[0] = cmd.input[TPCC_VAR_C_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb[2] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mdb::Row *r = tx.Query(tbl, mb, ROW_CUSTOMER);

         i32 oi = 0;
         tx.ReadColumn(r,
                          TPCC_COL_CUSTOMER_C_FIRST,
                          &output[TPCC_VAR_C_FIRST],
                          TXN_BYPASS);// read c_first
         tx.ReadColumn(r,
                          TPCC_COL_CUSTOMER_C_MIDDLE,
                          &output[TPCC_VAR_C_MIDDLE],
                          TXN_BYPASS);// read c_middle
         tx.ReadColumn(r,
                          TPCC_COL_CUSTOMER_C_LAST,
                          &output[TPCC_VAR_C_LAST],
                          TXN_BYPASS);// read c_last
         tx.ReadColumn(r,
                          TPCC_COL_CUSTOMER_C_BALANCE,
                          &output[TPCC_VAR_C_BALANCE],
                          TXN_BYPASS);// read c_balance

         *res = SUCCESS;
       }
  );

  // Ri order
  RegP(TPCC_ORDER_STATUS, TPCC_ORDER_STATUS_2,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_C_ID}, // i
       {}, // o
       {}, // c
       {TPCC_TB_ORDER, {TPCC_VAR_W_ID}},
       DF_NO,
       PROC {
         Log_debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_2);
         verify(cmd.input.size() >= 3);

         mdb::MultiBlob mb_0(3);
         mb_0[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb_0[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mb_0[2] = cmd.input[TPCC_VAR_C_ID].get_blob();
         mdb::Row
             *r_0 = tx.Query(tx.GetTable(TPCC_TB_ORDER_C_ID_SECONDARY),
                                mb_0,
                                ROW_ORDER_SEC);

         mdb::MultiBlob mb(3);
         mb[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mb[2] = r_0->get_blob(3); // FIXME add lock before reading

         mdb::Row *r = tx.Query(tx.GetTable(TPCC_TB_ORDER),
                                   mb,
                                   ROW_ORDER);
         tx.ReadColumn(r,
                          TPCC_COL_ORDER_O_ID,
                          &output[TPCC_VAR_O_ID],
                          TXN_BYPASS); // output[0] ==> o_id
         tx.ReadColumn(r,
                          TPCC_COL_ORDER_O_ENTRY_D,
                          &output[TPCC_VAR_O_ENTRY_D],
                          TXN_BYPASS); // output[1] ==> o_entry_d
         tx.ReadColumn(r,
                          TPCC_COL_ORDER_O_CARRIER_ID,
                          &output[TPCC_VAR_O_CARRIER_ID],
                          TXN_BYPASS); // output[2] ==> o_carrier_id
//        Log::debug("piece: %d, o_id: %d", TPCC_ORDER_STATUS_2, output[0].get_i32());
         *res = SUCCESS;
       }
  );

  // R order_line
  RegP(TPCC_ORDER_STATUS, TPCC_ORDER_STATUS_3,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_O_ID}, // i
       {}, // o
       {}, // c
       {TPCC_TB_ORDER_LINE, {TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
         Log_debug("TPCC_ORDER_STATUS, piece: %d", TPCC_ORDER_STATUS_3);
         verify(cmd.input.size() >= 3);
         mdb::MultiBlob mbl(4), mbh(4);
         Log_debug("ol_d_id: %d, ol_w_id: %d, ol_o_id: %d",
                   cmd.input[TPCC_VAR_O_ID].get_i32(),
                   cmd.input[TPCC_VAR_D_ID].get_i32(),
                   cmd.input[TPCC_VAR_W_ID].get_i32());
         mbl[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mbh[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mbl[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mbh[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mbl[2] = cmd.input[TPCC_VAR_O_ID].get_blob();
         mbh[2] = cmd.input[TPCC_VAR_O_ID].get_blob();
         Value ol_number_low(std::numeric_limits<i32>::min()),
             ol_number_high(std::numeric_limits<i32>::max());
         mbl[3] = ol_number_low.get_blob();
         mbh[3] = ol_number_high.get_blob();

         mdb::ResultSet rs = tx.QueryIn(tx.GetTable(TPCC_TB_ORDER_LINE),
                                           mbl,
                                           mbh,
                                           mdb::ORD_DESC,
                                           cmd.id_);
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

         std::vector<mdb::column_lock_t> column_locks;
         column_locks.reserve(5 * row_list.size());

         int i = 0;
         Log_debug("row_list size: %u", row_list.size());

         i = 0;
         i32 oi = 0;
         while (i < row_list.size()) {
           r = row_list[i++];
           tx.ReadColumn(r, TPCC_COL_ORDER_LINE_OL_I_ID,
                            &output[TPCC_VAR_OL_I_ID(i)], TXN_BYPASS);
           tx.ReadColumn(r, TPCC_COL_ORDER_LINE_OL_SUPPLY_W_ID,
                            &output[TPCC_VAR_OL_W_ID(i)], TXN_BYPASS);
           tx.ReadColumn(r, TPCC_COL_ORDER_LINE_OL_DELIVERY_D,
                            &output[TPCC_VAR_OL_DELIVER_D(i)], TXN_BYPASS);
           tx.ReadColumn(r, TPCC_COL_ORDER_LINE_OL_QUANTITY,
                            &output[TPCC_VAR_OL_QUANTITY(i)], TXN_BYPASS);
           tx.ReadColumn(r, TPCC_COL_ORDER_LINE_OL_AMOUNT,
                            &output[TPCC_VAR_OL_AMOUNTS(i)], TXN_BYPASS);
         }

         *res = SUCCESS;
       }
  );
}
} // namespace janus
