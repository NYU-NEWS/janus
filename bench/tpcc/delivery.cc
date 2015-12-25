#include "all.h"

namespace rococo {


void TpccChopper::delivery_init(TxnRequest &req) {
  /**
   * req.input_
   *  0       ==> w_id
   *  1       ==> o_carrier_id
   *  2       ==> d_id
   **/
  int d_id = req.input_[2].get_i32();
  n_pieces_all_ = 4;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  delivery_dep_.piece_new_orders = false;
  delivery_dep_.piece_orders = false;
  delivery_dep_.piece_order_lines = false;
  //delivery_dep_.d_cnt = (size_t)d_cnt;
  //delivery_dep_.piece_new_orders = (bool *)malloc(sizeof(bool) * d_cnt);
  //delivery_dep_.piece_orders = (bool *)malloc(sizeof(bool) * d_cnt);
  //delivery_dep_.piece_order_lines = (bool *)malloc(sizeof(bool) * d_cnt);
  //memset(delivery_dep_.piece_new_orders, false, sizeof(bool) * d_cnt);
  //memset(delivery_dep_.piece_orders, false, sizeof(bool) * d_cnt);
  //memset(delivery_dep_.piece_order_lines, false, sizeof(bool) * d_cnt);

  // piece 0, Ri & W new_order
  inputs_[0] = {
      {TPCC_VAR_W_ID, req.input_[0]},  // 0 ==>    no_w_id
      {TPCC_VAR_D_ID, req.input_[2]}  //  1 ==>    no_d_id
  };
  output_size_[0] = 1;
  p_types_[0] = TPCC_DELIVERY_0;
  delivery_shard(TPCC_TB_NEW_ORDER, req.input_, sharding_[0], d_id);
  status_[0] = READY;

  // piece 1, Ri & W order
  inputs_[1] = {
      {TPCC_VAR_O_ID,         Value()},        // 0 ==>    o_id,   depends on piece 0
      {TPCC_VAR_W_ID,         req.input_[0]},  // 1 ==>    o_w_id
      {TPCC_VAR_D_ID,         req.input_[2]},  // 2 ==>    o_d_id
      {TPCC_VAR_O_CARRIER_ID, req.input_[1]}   // 3 ==>    o_carrier_id
  };
  output_size_[1] = 1;
  p_types_[1] = TPCC_DELIVERY_1;
  delivery_shard(TPCC_TB_ORDER, req.input_, sharding_[1], d_id);
  status_[1] = WAITING;

  // piece 2, Ri & W order_line
  inputs_[2] = {
      {TPCC_VAR_O_ID, Value()},        // 0 ==>    ol_o_id,   depends on piece 0
      {TPCC_VAR_W_ID, req.input_[0]},  // 1 ==>    ol_w_id
      {TPCC_VAR_D_ID, req.input_[2]}   // 2 ==>    ol_d_id
  };
  output_size_[2] = 1;
  p_types_[2] = TPCC_DELIVERY_2;
  delivery_shard(TPCC_TB_ORDER_LINE, req.input_, sharding_[2], d_id);
  status_[2] = WAITING;

  // piece 3, W customer
  inputs_[3] = {
      {TPCC_VAR_C_ID, Value()},        // 0 ==>    c_id,   depends on piece 1
      {TPCC_VAR_W_ID, req.input_[0]},  // 1 ==>    c_w_id
      {TPCC_VAR_D_ID, req.input_[2]},  // 2 ==>    c_d_id
      {TPCC_VAR_OL_AMOUNT, Value()}    // 3 ==>    ol_amount, depends on piece 2
  };
  output_size_[3] = 0;
  p_types_[3] = TPCC_DELIVERY_3;
  delivery_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[3], d_id);
  status_[3] = WAITING;
}


void TpccChopper::delivery_shard(const char *tb,
                                 const std::vector<mdb::Value> &input,
                                 uint32_t &site,
                                 int cnt) {
  MultiValue mv;
  if (tb == TPCC_TB_NEW_ORDER
      || tb == TPCC_TB_ORDER
      || tb == TPCC_TB_ORDER_LINE
      || tb == TPCC_TB_CUSTOMER)
    // based on w_id
    mv = MultiValue(input[0]);
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::delivery_retry() {
  status_[0] = READY;
  status_[1] = WAITING;
  status_[2] = WAITING;
  status_[3] = WAITING;
  delivery_dep_.piece_new_orders = false;
  delivery_dep_.piece_orders = false;
  delivery_dep_.piece_order_lines = false;
}


void TpccPiece::reg_delivery() {
  BEGIN_PIE(TPCC_DELIVERY,
          TPCC_DELIVERY_0, // Ri & W new_order
          DF_FAKE) {
    // this is a little bit tricky, the first half will do most of the job,
    // removing the row from the table, but it won't actually release the
    // resource. And the bottom half is in charge of release the resource,
    // including the vertex entry

    Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_0);
    verify(input.size() == 2);
    Value buf;
    mdb::Txn *txn = dtxn->mdb_txn_;
    //cell_locator_t cl(TPCC_TB_NEW_ORDER, 3);
    mdb::Row *r = NULL;
    mdb::Table *tbl = dtxn->GetTable(TPCC_TB_NEW_ORDER);

    mdb::MultiBlob mbl(3), mbh(3);
    mbl[0] = input[TPCC_VAR_D_ID].get_blob();
    mbh[0] = input[TPCC_VAR_D_ID].get_blob();
    mbl[1] = input[TPCC_VAR_W_ID].get_blob();
    mbh[1] = input[TPCC_VAR_W_ID].get_blob();
    Value no_o_id_low(std::numeric_limits<i32>::min()),
            no_o_id_high(std::numeric_limits<i32>::max());
    mbl[2] = no_o_id_low.get_blob();
    mbh[2] = no_o_id_high.get_blob();

    mdb::ResultSet rs = dtxn->QueryIn(tbl,
                                      mbl,
                                      mbh,
                                      mdb::ORD_ASC,
                                      header.pid);
    if (rs.has_next()) {
      r = rs.next();
      TPL_KISS_ROW(r);
      dtxn->ReadColumn(r, TPCC_COL_NEW_ORDER_NO_W_ID, &buf);
      output[TPCC_VAR_O_ID] = buf;
    } else {
      TPL_KISS_NONE;
      output[TPCC_VAR_O_ID] = Value((i32) -1);
    }
    if (r) {
      txn->remove_row(tbl, r);
    }

//    if (IS_MODE_RCC || IS_MODE_RO6) { // deptran
//      if (IN_PHASE_1) { // deptran start req, top half
//        if (r) { // if find a row
//          // FIXME!!!!!
//          RCC_KISS(r, 0, true);
//          RCC_KISS(r, 1, true);
//          RCC_KISS(r, 2, true);
//          static int iiiii = 0;
//          RCC_SAVE_ROW(r, iiiii++);
//          tbl->remove(r, false); // don't release the row
//        }
//      } else { // deptran finish
//        auto &row_map = ((RCCDTxn*)dtxn)->dreqs_.back().row_map;
//        for (auto &it : row_map) {
//          it.second->release();
//        }
//      }
//    } else { // non deptran
//      if (r) {
//        txn->remove_row(tbl, r);
//      }
//    }

    *res = SUCCESS;
    return;
  } END_PIE

  BEGIN_CB(TPCC_DELIVERY, 0)
    TpccChopper *tpcc_ch = (TpccChopper*) ch;
    verify(output.size() == 1);
    verify(!tpcc_ch->delivery_dep_.piece_new_orders);
    if (output[TPCC_VAR_O_ID].get_i32() == (i32) -1) { // new_order not found
      tpcc_ch->status_[1] = FINISHED;
      tpcc_ch->status_[2] = FINISHED;
      tpcc_ch->status_[3] = FINISHED;
      tpcc_ch->n_pieces_out_ += 3;
      Log::info("TPCC DELIVERY: no more new order for w_id: %d, d_id: %d",
                tpcc_ch->inputs_[0][TPCC_VAR_W_ID].get_i32(),
                tpcc_ch->inputs_[0][TPCC_VAR_D_ID].get_i32());
      return false;
    } else {
      tpcc_ch->inputs_[1][TPCC_VAR_O_ID] = output[TPCC_VAR_O_ID];
      tpcc_ch->inputs_[2][TPCC_VAR_O_ID] = output[TPCC_VAR_O_ID];
      tpcc_ch->status_[1] = READY;
      tpcc_ch->status_[2] = READY;
      tpcc_ch->delivery_dep_.piece_new_orders = true;
      return true;
    }
  END_CB

  BEGIN_PIE(TPCC_DELIVERY,
          TPCC_DELIVERY_1, // Ri & W order
          DF_NO) {

    Log_debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_1);
    verify(input.size() == 4);
    i32 oi = 0;
    Value buf;
    mdb::Txn *txn = dtxn->mdb_txn_;
    mdb::MultiBlob mb(3);
    //cell_locator_t cl(TPCC_TB_ORDER, 3);
    mb[0] = input[TPCC_VAR_D_ID].get_blob();
    mb[1] = input[TPCC_VAR_W_ID].get_blob();
    mb[2] = input[TPCC_VAR_O_ID].get_blob();
    //Log::debug("Delivery: o_d_id: %d, o_w_id: %d, o_id: %d, hash: %u", input[2].get_i32(), input[1].get_i32(), input[0].get_i32(), mdb::MultiBlob::hash()(cl.primary_key));
    mdb::Row *r = dtxn->Query(txn->get_table(TPCC_TB_ORDER), mb, ROW_ORDER);
    dtxn->ReadColumn(r,
                     TPCC_COL_ORDER_O_C_ID,
                     &output[TPCC_VAR_C_ID],
                     TXN_BYPASS); // read o_c_id
    dtxn->WriteColumn(r,
                      TPCC_COL_ORDER_O_CARRIER_ID,
                      input[TPCC_VAR_O_CARRIER_ID],
                      TXN_INSTANT); // write o_carrier_id

    *res = SUCCESS;
    return;
  } END_PIE

  BEGIN_CB(TPCC_DELIVERY, 1)
    TpccChopper *tpcc_ch = (TpccChopper*) ch;
    verify(output.size() == 1);
    tpcc_ch->inputs_[3][TPCC_VAR_C_ID] = output[TPCC_VAR_C_ID];
    tpcc_ch->delivery_dep_.piece_orders = true;
    if (tpcc_ch->delivery_dep_.piece_order_lines) {
      tpcc_ch->status_[3] = READY;
      return true;
    } else {
      return false;
    }
  END_CB

  BEGIN_PIE(TPCC_DELIVERY,
          TPCC_DELIVERY_2, // Ri & W order_line
          DF_NO) {
    Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_2);
    verify(input.size() == 3);
    //        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
    mdb::MultiBlob mbl(4), mbh(4);
    mbl[0] = input[TPCC_VAR_D_ID].get_blob();
    mbh[0] = input[TPCC_VAR_D_ID].get_blob();
    mbl[1] = input[TPCC_VAR_W_ID].get_blob();
    mbh[1] = input[TPCC_VAR_W_ID].get_blob();
    mbl[2] = input[TPCC_VAR_O_ID].get_blob();
    mbh[2] = input[TPCC_VAR_O_ID].get_blob();
    Value ol_number_low(std::numeric_limits<i32>::min()),
          ol_number_high(std::numeric_limits<i32>::max());
    mbl[3] = ol_number_low.get_blob();
    mbh[3] = ol_number_high.get_blob();

    mdb::ResultSet rs = dtxn->QueryIn(dtxn->GetTable(TPCC_TB_ORDER_LINE),
                                    mbl,
                                    mbh,
                                    mdb::ORD_ASC,
                                    header.pid);
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

    std::vector<mdb::column_lock_t> column_locks;
    column_locks.reserve(2 * row_list.size());

    int i = 0;
    while (i < row_list.size()) {
      r = row_list[i++];
      Value buf(0.0);
      dtxn->ReadColumn(r, TPCC_COL_ORDER_LINE_OL_AMOUNT,
                       &buf, TXN_INSTANT); // read ol_amount
      ol_amount_buf += buf.get_double();
      dtxn->WriteColumn(r, TPCC_COL_ORDER_LINE_OL_DELIVERY_D,
                        Value(std::to_string(time(NULL))),
                        TXN_INSTANT);
    }
    output[TPCC_VAR_OL_AMOUNT] = Value(ol_amount_buf);

    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_DELIVERY, 2)
    TpccChopper *tpcc_ch = (TpccChopper*) ch;
    verify(output.size() == 1);
    tpcc_ch->inputs_[3][TPCC_VAR_OL_AMOUNT] = output[TPCC_VAR_OL_AMOUNT];
    tpcc_ch->delivery_dep_.piece_order_lines = true;
    if (tpcc_ch->delivery_dep_.piece_orders) {
      tpcc_ch->status_[3] = READY;
      return true;
    } else {
      return false;
    }
  END_CB

  BEGIN_PIE(TPCC_DELIVERY,
          TPCC_DELIVERY_3, // W customer
          DF_REAL) {
    Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_3);
    verify(input.size() == 4);
    mdb::Row *r = NULL;
    mdb::MultiBlob mb(3);
    //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
    mb[0] = input[TPCC_VAR_C_ID].get_blob();
    mb[1] = input[TPCC_VAR_D_ID].get_blob();
    mb[2] = input[TPCC_VAR_W_ID].get_blob();

    r = dtxn->Query(dtxn->GetTable(TPCC_TB_CUSTOMER),
                    mb,
                    ROW_CUSTOMER);
    Value buf(0.0);
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_BALANCE, &buf);
    buf.set_double(buf.get_double() + input[TPCC_VAR_OL_AMOUNT].get_double());
    dtxn->WriteColumn(r, TPCC_COL_CUSTOMER_C_BALANCE,
                      buf, TXN_DEFERRED);
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_DELIVERY_CNT,
                     &buf, TXN_BYPASS);
    buf.set_i32(buf.get_i32() + (i32) 1);
    dtxn->WriteColumn(r, TPCC_COL_CUSTOMER_C_DELIVERY_CNT,
                      buf, TXN_DEFERRED);
    *res = SUCCESS;
  } END_PIE
}

} // namespace rococo
