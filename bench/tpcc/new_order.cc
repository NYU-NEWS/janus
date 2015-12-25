#include "all.h"

namespace rococo {

static uint32_t TXN_TYPE = TPCC_NEW_ORDER;

void TpccChopper::new_order_init(TxnRequest &req) {
  new_order_dep_ = new_order_dep_t();
  /**
   * req.input_
   *  0       ==> w_id, d_w_id, c_w_id
   *  1       ==> d_id, c_d_id
   *  2       ==> c_id
   *  3       ==> ol_cnt
   *  4 + 3*i ==> s_i_id, ol_i_id, i_id
   *  5 + 3*i ==> ol_supply_w_id
   *  6 + 3*i ==> ol_quantity
   **/
  int32_t ol_cnt = req.input_[3].get_i32();

  new_order_dep_.piece_0_dist = false;
  new_order_dep_.ol_cnt = (size_t) ol_cnt;
  new_order_dep_.piece_items = (bool *) malloc(sizeof(bool) * ol_cnt);
  new_order_dep_.piece_stocks = (bool *) malloc(sizeof(bool) * ol_cnt);
  memset(new_order_dep_.piece_items, false, sizeof(bool) * ol_cnt);
  memset(new_order_dep_.piece_stocks, false, sizeof(bool) * ol_cnt);

  n_pieces_all_ = 5 + 4 * ol_cnt;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  // piece 0, Ri&W district
  inputs_[0] = {
      {TPCC_VAR_W_ID, req.input_[0]},  // 0 ==> d_w_id
      {TPCC_VAR_D_ID, req.input_[1]}   // 1 ==> d_id
  };
  output_size_[0] = 2;
  p_types_[0] = TPCC_NEW_ORDER_0;
  new_order_shard(TPCC_TB_DISTRICT,
                  req.input_,  // sharding based on d_w_id
                  sharding_[0]);
  status_[0] = READY;

  // piece 1, R warehouse
  inputs_[1] = {
      {TPCC_VAR_W_ID, req.input_[0]}   // 0 ==> w_id
  };
  output_size_[1] = 1;
  p_types_[1] = TPCC_NEW_ORDER_1;
  new_order_shard(TPCC_TB_WAREHOUSE,
                  req.input_,  // sharding based on w_id
                  sharding_[1]);
  status_[1] = READY;

  // piece 2, R customer
  inputs_[2] = {
      {TPCC_VAR_W_ID, req.input_[0]},  // 0 ==> c_w_id
      {TPCC_VAR_D_ID, req.input_[1]},  // 1 ==> c_d_id
      {TPCC_VAR_C_ID, req.input_[2]}   // 2 ==> c_id
  };
  output_size_[2] = 3;
  p_types_[2] = TPCC_NEW_ORDER_2;
  new_order_shard(TPCC_TB_CUSTOMER,
                  req.input_,  // sharding based on c_w_id
                  sharding_[2]);
  status_[2] = READY;

  bool all_local = true;
  for (int i = 0; i < ol_cnt; i++) {
    Value is_remote((i32) 0);
    if (req.input_[5 + 3 * i] != req.input_[0]) {
      all_local = false;
      is_remote.set_i32((i32) 1);
    }
    // piece 5 + 4 * i, Ri item
    inputs_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = {
        {TPCC_VAR_I_ID(i), req.input_[4 + 3 * i]}   // 0 ==> i_id
    };
    output_size_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = 3;
    p_types_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = TPCC_NEW_ORDER_RI(i);
    new_order_shard(TPCC_TB_ITEM,
                    req.input_,
                    sharding_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)],
                    i);
    status_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = READY;

    // piece 6 + 4 * i, Ri stock
    inputs_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = {
        {TPCC_VAR_I_ID(i), req.input_[4 + 3 * i]},  // 0 ==> s_i_id
        {TPCC_VAR_S_W_ID(i), req.input_[5 + 3 * i]},  // 1 ==> ol_supply_w_id / s_w_id
        {TPCC_VAR_S_D_ID(i), req.input_[1]}           // 2 ==> d_id
    };
    output_size_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = 2;
    p_types_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = TPCC_NEW_ORDER_RS(i);
    new_order_shard(TPCC_TB_STOCK,
                    req.input_,
                    sharding_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)],
                    i);
    status_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = READY;

    // piece 7 + 4 * i, W stock
    inputs_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = {
        {TPCC_VAR_I_ID(i), req.input_[4 + 3 * i]},  // 0 ==> s_i_id
        {TPCC_VAR_S_W_ID(i), req.input_[5 + 3 * i]},  // 1 ==> ol_supply_w_id / s_w_id
        {TPCC_VAR_OL_QUANTITY(i), req.input_[6 + 3 * i]},  // 2 ==> ol_quantity
        {TPCC_VAR_S_REMOTE_CNT(i), is_remote}               // 3 ==> increase delta s_remote_cnt
    };
    output_size_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = 0;
    p_types_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = TPCC_NEW_ORDER_WS(i);
    new_order_shard(TPCC_TB_STOCK,
                    req.input_,
                    sharding_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)],
                    i);
    status_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = READY;

    // piece 8 + 4 * i, W order_line, depends on piece 0 & 5+3*i & 6+3*i
    inputs_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = {
        {TPCC_VAR_OL_D_ID(i),         req.input_[1]},           // 0 ==> ol_d_id
        {TPCC_VAR_OL_W_ID(i),         req.input_[0]},           // 1 ==> ol_w_id
        {TPCC_VAR_OL_O_ID(i),         Value((i32) 0)},          // 2 ==> ol_o_id    depends on piece 0
        {TPCC_VAR_OL_NUMBER(i),       Value((i32) i)},          // 3 ==> ol_number
        {TPCC_VAR_I_ID(i),            req.input_[4 + 3 * i]},   // 4 ==> ol_i_id
        {TPCC_VAR_S_W_ID(i),          req.input_[5 + 3 * i]},   // 5 ==> ol_supply_w_id
        {TPCC_VAR_OL_DELIVER_D(i),    Value(std::string())},    // 6 ==> ol_deliver_d
        {TPCC_VAR_OL_QUANTITY(i),     req.input_[6 + 3 * i]},   // 7 ==> ol_quantity
        {TPCC_VAR_OL_AMOUNTS(i),      Value((double) 0.0)},     // 8 ==> ol_amount  depends on piece 5+3*i
        {TPCC_VAR_OL_DIST_INFO(i),    Value(std::string())},    // 9 ==> ol_dist_info depends on piece 6+3*i
    };
    output_size_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = 0;
    p_types_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = TPCC_NEW_ORDER_WOL(i);
    new_order_shard(TPCC_TB_ORDER_LINE,
                    req.input_,// sharding based on ol_w_id
                    sharding_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)]);
    status_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = WAITING;
  }
  // piece 3, W order, depends on piece 0
  inputs_[3] = {
      {TPCC_VAR_O_ID, Value((i32) 0)}, // 0 ==> // o_id   depends on piece 0
      {TPCC_VAR_D_ID, req.input_[1]},  // 1 ==> // o_d_id
      {TPCC_VAR_W_ID, req.input_[0]},  // 2 ==> // o_w_id
      {TPCC_VAR_C_ID, req.input_[2]},  // 3 ==> // o_c_id
      {TPCC_VAR_O_CARRIER_ID, Value((i32) 0)}, // 4 ==> // o_carrier_id
      {TPCC_VAR_OL_CNT, req.input_[3]},  // 5 ==> // o_ol_cnt
      {TPCC_VAR_O_ALL_LOCAL, all_local ? Value((i32) 1) : Value((i32) 0) }// 6 ==> // o_all_local
  };
  output_size_[3] = 0;
  p_types_[3] = TPCC_NEW_ORDER_3;
  new_order_shard(TPCC_TB_ORDER,
                  req.input_, // sharding based on o_w_id
                  sharding_[3]);
  status_[3] = WAITING;

  // piece 4, W new_order, depends on piece 0
  inputs_[4] = {
      {TPCC_VAR_O_ID, Value((i32) 0)},         // 0 ==> no_id   depends on piece 0
      {TPCC_VAR_D_ID, req.input_[1]},          // 1 ==> no_d_id
      {TPCC_VAR_W_ID, req.input_[0]},          // 2 ==> no_w_id
  };
  output_size_[4] = 0;
  p_types_[4] = TPCC_NEW_ORDER_4;
  new_order_shard(TPCC_TB_NEW_ORDER,
                  req.input_, // sharding based on no_w_id
                  sharding_[4]);
  status_[4] = WAITING;
}

void TpccChopper::new_order_shard(const char *tb,
                                  const std::vector<Value> &input,
                                  uint32_t &site,
                                  int cnt) {
  // partition based on w_id
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT  ||
      tb == TPCC_TB_WAREHOUSE ||
      tb == TPCC_TB_CUSTOMER  ||
      tb == TPCC_TB_ORDER     ||
      tb == TPCC_TB_NEW_ORDER ||
      tb == TPCC_TB_ORDER_LINE)
    mv = MultiValue(input[0]);
  else if (tb == TPCC_TB_ITEM)
    // based on i_id
    mv = MultiValue(input[4 + 3 * cnt]);
  else if (tb == TPCC_TB_STOCK)
    // based on s_w_id
    mv = MultiValue(input[5 + 3 * cnt]);
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::new_order_retry() {
  status_[0] = READY;
  status_[1] = READY;
  status_[2] = READY;
  status_[3] = WAITING;
  status_[4] = WAITING;
  new_order_dep_.piece_0_dist = false;
  for (size_t i = 0; i < new_order_dep_.ol_cnt; i++) {
    new_order_dep_.piece_items[i] = false;
    new_order_dep_.piece_stocks[i] = false;
    status_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = READY;
    status_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = READY;
    status_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = READY;
    status_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = WAITING;
  }
}

void TpccPiece::reg_new_order() {
  BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_0, // Ri & W district
            DF_NO) {
    verify(input.size() == 2);
    mdb::MultiBlob mb(2);
    mb[0] = input[TPCC_VAR_D_ID].get_blob();
    mb[1] = input[TPCC_VAR_W_ID].get_blob();
    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_DISTRICT),
                              mb,
                              ROW_DISTRICT);
    Value buf(0);
    // R district
    dtxn->ReadColumn(r,
                     TPCC_COL_DISTRICT_D_TAX,
                     &output[TPCC_VAR_D_TAX],
                     TXN_BYPASS);
    dtxn->ReadColumn(r,
                     TPCC_COL_DISTRICT_D_NEXT_O_ID,
                     &buf,
                     TXN_BYPASS); // read d_next_o_id
    output[TPCC_VAR_D_NEXT_O_ID] = buf;
    // read d_next_o_id, increment by 1
    buf.set_i32((i32)(buf.get_i32() + 1));
    dtxn->WriteColumn(r,
                      TPCC_COL_DISTRICT_D_NEXT_O_ID,
                      buf,
                      TXN_INSTANT);
    return;
  } END_PIE

  BEGIN_CB(TPCC_NEW_ORDER, 0)
    TpccChopper* tpcc_ch = (TpccChopper*) ch;
    tpcc_ch->new_order_dep_.piece_0_dist = true;
    Value o_id = output[TPCC_VAR_D_NEXT_O_ID];
    tpcc_ch->inputs_[3][TPCC_VAR_O_ID] = o_id;
    tpcc_ch->inputs_[4][TPCC_VAR_O_ID] = o_id;
    tpcc_ch->status_[3] = READY;
    tpcc_ch->status_[4] = READY;

    for (size_t i = 0; i < tpcc_ch->new_order_dep_.ol_cnt; i++) {
      auto pi = TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i);
      tpcc_ch->inputs_[pi][TPCC_VAR_OL_O_ID(i)] = o_id;
      if (tpcc_ch->new_order_dep_.piece_items[i] &&
          tpcc_ch->new_order_dep_.piece_stocks[i])
        tpcc_ch->status_[pi] = READY;
    }

    return true;
  END_CB

  BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_1, // R warehouse
            DF_NO) {
    verify(input.size() == 1);
    Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_1);
    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_WAREHOUSE),
                              input[TPCC_VAR_W_ID].get_blob(),
                              ROW_WAREHOUSE);
    // R warehouse
    dtxn->ReadColumn(r, TPCC_COL_WAREHOUSE_W_TAX,
                     &output[TPCC_VAR_W_TAX], TXN_BYPASS); // read w_tax
    return;
  } END_PIE

  BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_2, // R customer
            DF_NO //XXX either i or d is ok
  ) {
    verify(input.size() == 3);
    Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_2);

    mdb::MultiBlob mb(3);
    mb[0] = input[TPCC_VAR_C_ID].get_blob();
    mb[1] = input[TPCC_VAR_D_ID].get_blob();
    mb[2] = input[TPCC_VAR_W_ID].get_blob();
    auto table = dtxn->GetTable(TPCC_TB_CUSTOMER);
    mdb::Row *r = dtxn->Query(table, mb, ROW_CUSTOMER);
    // R customer
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_LAST,
                     &output[TPCC_VAR_C_LAST], TXN_BYPASS);
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_CREDIT,
                     &output[TPCC_VAR_C_CREDIT], TXN_BYPASS);
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_DISCOUNT,
                     &output[TPCC_VAR_C_DISCOUNT], TXN_BYPASS);

    return;
  } END_PIE

  BEGIN_PIE(TPCC_NEW_ORDER,
            TPCC_NEW_ORDER_3, // W order
            DF_REAL) {
    verify(input.size() == 7);
    Log::debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_3);
    i32 oi = 0;
    mdb::Table *tbl = dtxn->GetTable(TPCC_TB_ORDER);

    mdb::MultiBlob mb(3);
    mb[0] = input[TPCC_VAR_D_ID].get_blob();
    mb[1] = input[TPCC_VAR_W_ID].get_blob();
    mb[2] = input[TPCC_VAR_C_ID].get_blob();

    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_ORDER_C_ID_SECONDARY),
                              mb,
                              ROW_ORDER_SEC);
    verify(r);
    verify(r->schema_);

    // W order
    std::vector<Value> row_data= {
        input[TPCC_VAR_D_ID],
        input[TPCC_VAR_W_ID],
        input[TPCC_VAR_O_ID],
        input[TPCC_VAR_C_ID],
        Value(std::to_string(time(NULL))),  // o_entry_d
        input[TPCC_VAR_O_CARRIER_ID],
        input[TPCC_VAR_OL_CNT],
        input[TPCC_VAR_O_ALL_LOCAL]
    };
    CREATE_ROW(tbl->schema(), row_data);
//    verify(r->schema_);
//    RCC_KISS(r, 0, false);
//    RCC_KISS(r, 1, false);
//    RCC_KISS(r, 2, false);
//    RCC_KISS(r, 5, false);
//    RCC_SAVE_ROW(r, TPCC_NEW_ORDER_3);
//    RCC_PHASE1_RET;
//    RCC_LOAD_ROW(r, TPCC_NEW_ORDER_3);

    verify(r->schema_);
    dtxn->InsertRow(tbl, r);

    // write TPCC_TB_ORDER_C_ID_SECONDARY
    //mdb::MultiBlob mb(3);
    //mb[0] = input[1].get_blob();
    //mb[1] = input[2].get_blob();
    //mb[2] = input[3].get_blob();
    r = dtxn->Query(dtxn->GetTable(TPCC_TB_ORDER_C_ID_SECONDARY),
                    mb,
                    ROW_ORDER_SEC);
    dtxn->WriteColumn(r, 3, input[TPCC_VAR_W_ID]);
    return;
  } END_PIE

  BEGIN_PIE(TPCC_NEW_ORDER,
          TPCC_NEW_ORDER_4, // W new_order
          DF_REAL) {
    verify(input.size() == 3);
    Log_debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_4);

    mdb::Table *tbl = dtxn->GetTable(TPCC_TB_NEW_ORDER);
    mdb::Row *r = NULL;

    // W new_order
    std::vector<Value> row_data({
            input[TPCC_VAR_D_ID],   // o_d_id
            input[TPCC_VAR_W_ID],   // o_w_id
            input[TPCC_VAR_O_ID],   // o_id
    });

    CREATE_ROW(tbl->schema(), row_data);

//    RCC_KISS(r, 0, false);
//    RCC_KISS(r, 1, false);
//    RCC_KISS(r, 2, false);

    dtxn->InsertRow(tbl, r);
    *res = SUCCESS;
    return;
  } END_PIE

  BEGIN_LOOP_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_RI(0), 1000, DF_NO)
    verify(input.size() == 1);
    Log_debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_RI(I));
    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_ITEM),
                              input[TPCC_VAR_I_ID(I)].get_blob(),
                              ROW_ITEM);
    // Ri item
    dtxn->ReadColumn(r,
                     TPCC_COL_ITEM_I_NAME,
                     &output[TPCC_VAR_I_NAME(I)],
                     TXN_BYPASS);
    dtxn->ReadColumn(r,
                     TPCC_COL_ITEM_I_PRICE,
                     &output[TPCC_VAR_I_PRICE(I)],
                     TXN_BYPASS);
    dtxn->ReadColumn(r,
                     TPCC_COL_ITEM_I_DATA,
                     &output[TPCC_VAR_I_DATA(I)],
                     TXN_BYPASS);

    *res = SUCCESS;
    return;
  END_LOOP_PIE


  for (int i = 0; i < 1000; i++) { // 1000 is a magical number?
    if (TPCC_NEW_ORDER_IS_ITEM_INDEX(i)) {
      txn_reg_->callbacks_[std::make_pair(TPCC_NEW_ORDER, i)] = \
        [i](TxnChopper *ch, std::map<int32_t, Value> output) -> bool {
        TpccChopper *tpcc_ch = (TpccChopper*) ch;
        auto ol_pi = TPCC_NEW_ORDER_INDEX_ITEM_TO_ORDER_LINE(i);
        auto ol_i = TPCC_NEW_ORDER_INDEX_ITEM_TO_CNT(i);
        auto s_pi = TPCC_NEW_ORDER_INDEX_ITEM_TO_DEFER_STOCK(i);
        tpcc_ch->inputs_[ol_pi][TPCC_VAR_OL_AMOUNTS(ol_i)] =
            Value((double) (output[TPCC_VAR_I_PRICE(ol_i)].get_double()
            * tpcc_ch->inputs_[s_pi][TPCC_VAR_OL_QUANTITY(ol_i)].get_i32()));
        tpcc_ch->new_order_dep_.piece_items[ol_i] = true;
        if (tpcc_ch->new_order_dep_.piece_0_dist
            && tpcc_ch->new_order_dep_.piece_stocks[ol_i]) {
          tpcc_ch->status_[ol_pi] = READY;
          return true;
        } else {
          return false;
        }
      };
    }
  }

  BEGIN_LOOP_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_RS(0), 1000, DF_NO)
    verify(input.size() == 3);
    Log_debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_RS(I));
    mdb::MultiBlob mb(2);
    mb[0] = input[TPCC_VAR_I_ID(I)].get_blob();
    mb[1] = input[TPCC_VAR_S_W_ID(I)].get_blob();
    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_STOCK),
                              mb,
                              ROW_STOCK);
    verify(r->schema_);
    //i32 s_dist_col = 3 + input[2].get_i32();
    // Ri stock
    // FIXME compress all s_dist_xx into one column
    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_DIST_XX,
                     &output[TPCC_VAR_S_DIST_XX(I)], TXN_BYPASS); // 0 ==> s_dist_xx
    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_DATA,
                     &output[TPCC_VAR_S_DATA(I)], TXN_BYPASS); // 1 ==> s_data
    *res = SUCCESS;
    return;
  END_LOOP_PIE

  for (int i = 0; i < 1000; i++) { // 1000 is a magical number?
    if (TPCC_NEW_ORDER_IS_IM_STOCK_INDEX(i)) {
      txn_reg_->callbacks_[std::make_pair(TPCC_NEW_ORDER, i)] = \
        [i](TxnChopper *ch, std::map<int32_t, Value> output) -> bool {
        TpccChopper *tpcc_ch = (TpccChopper*) ch;
        auto ol_pi = TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_ORDER_LINE(i);
        auto ol_i = TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_CNT(i);
        tpcc_ch->inputs_[ol_pi][TPCC_VAR_OL_DIST_INFO(ol_i)] =
            output[TPCC_VAR_S_DIST_XX(ol_i)];
        tpcc_ch->new_order_dep_.piece_stocks[ol_i] = true;
        if (tpcc_ch->new_order_dep_.piece_0_dist &&
            tpcc_ch->new_order_dep_.piece_items[ol_i]) {
          tpcc_ch->status_[ol_pi] = READY;
          return true;
        } else {
          return false;
        }
      };
    }
  }

  BEGIN_LOOP_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_WS(0), 1000, DF_REAL)
    verify(input.size() == 4);
    Log_debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_WS(I));
    mdb::Row *r = NULL;
    mdb::MultiBlob mb(2);
    mb[0] = input[TPCC_VAR_I_ID(I)].get_blob();
    mb[1] = input[TPCC_VAR_S_W_ID(I)].get_blob();

    r = dtxn->Query(dtxn->GetTable(TPCC_TB_STOCK), mb, ROW_STOCK_TEMP);
    verify(r->schema_);

//    RCC_KISS(r, 2, false);
//    RCC_KISS(r, 13, false);
//    RCC_KISS(r, 14, false);
//    RCC_KISS(r, 15, false);
//    RCC_SAVE_ROW(r, TPCC_NEW_ORDER_7);
//    RCC_PHASE1_RET;
//    RCC_LOAD_ROW(r, TPCC_NEW_ORDER_7);

    // Ri stock
    Value buf(0);
    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_QUANTITY, &buf);
    int32_t new_ol_quantity = buf.get_i32() -
        input[TPCC_VAR_OL_QUANTITY(I)].get_i32();

    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_YTD, &buf);
    Value new_s_ytd(buf.get_i32() +
        input[TPCC_VAR_OL_QUANTITY(I)].get_i32());

    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_ORDER_CNT, &buf);
    Value new_s_order_cnt((i32)(buf.get_i32() + 1));

    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_REMOTE_CNT, &buf);
    Value new_s_remote_cnt(buf.get_i32() +
        input[TPCC_VAR_S_REMOTE_CNT(I)].get_i32());

    if (new_ol_quantity < 10)
      new_ol_quantity += 91;
    Value new_ol_quantity_value(new_ol_quantity);

    dtxn->WriteColumns(r,
                       {
                           TPCC_COL_STOCK_S_QUANTITY,
                           TPCC_COL_STOCK_S_YTD,
                           TPCC_COL_STOCK_S_ORDER_CNT,
                           TPCC_COL_STOCK_S_REMOTE_CNT
                       },
                       {
                           new_ol_quantity_value,
                           new_s_ytd,
                           new_s_order_cnt,
                           new_s_remote_cnt
                       });
    *res = SUCCESS;
    return;
  END_LOOP_PIE

  BEGIN_LOOP_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_WOL(0), 1000, DF_REAL) {
    verify(input.size() == 10);
    Log_debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_WOL(I));

    mdb::Table *tbl = dtxn->GetTable(TPCC_TB_ORDER_LINE);
    mdb::Row *r = NULL;

    CREATE_ROW(tbl->schema(), vector<Value>({
      input[TPCC_VAR_OL_D_ID(I)],
      input[TPCC_VAR_OL_W_ID(I)],
      input[TPCC_VAR_OL_O_ID(I)],
      input[TPCC_VAR_OL_NUMBER(I)],
      input[TPCC_VAR_I_ID(I)],
      input[TPCC_VAR_S_W_ID(I)],
      input[TPCC_VAR_OL_DELIVER_D(I)],
      input[TPCC_VAR_OL_QUANTITY(I)],
      input[TPCC_VAR_OL_AMOUNTS(I)],
      input[TPCC_VAR_OL_DIST_INFO(I)]
    }));


//    RCC_KISS(r, 0, false);
//    RCC_KISS(r, 1, false);
//    RCC_KISS(r, 2, false);
//    RCC_KISS(r, 3, false);
//    RCC_KISS(r, 4, false);
//    RCC_KISS(r, 6, false);
//    RCC_KISS(r, 8, false);
//    RCC_SAVE_ROW(r, TPCC_NEW_ORDER_8);
//    RCC_PHASE1_RET;
//    RCC_LOAD_ROW(r, TPCC_NEW_ORDER_8);
        
    dtxn->InsertRow(tbl, r);
    *res = SUCCESS;
    return;
  } END_LOOP_PIE
}
} // namespace rococo
