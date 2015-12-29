#include "all.h"
#include "generator.h"

namespace rococo {

static uint32_t TXN_TYPE = TPCC_NEW_ORDER;

void TpccChopper::NewOrderInit(TxnRequest &req) {
  new_order_dep_ = new_order_dep_t();
  int32_t ol_cnt = req.input_[TPCC_VAR_OL_CNT].get_i32();

  new_order_dep_.piece_0_dist = false;
  new_order_dep_.ol_cnt = (size_t) ol_cnt;
  new_order_dep_.piece_items = (bool *) malloc(sizeof(bool) * ol_cnt);
  new_order_dep_.piece_stocks = (bool *) malloc(sizeof(bool) * ol_cnt);
  memset(new_order_dep_.piece_items, false, sizeof(bool) * ol_cnt);
  memset(new_order_dep_.piece_stocks, false, sizeof(bool) * ol_cnt);

  n_pieces_all_ = 5 + 4 * ol_cnt;

  // piece 0, Ri&W district
  inputs_[TPCC_NEW_ORDER_0] = {
      {TPCC_VAR_W_ID, req.input_[TPCC_VAR_W_ID]},
      {TPCC_VAR_D_ID, req.input_[TPCC_VAR_D_ID]}
  };
  output_size_[TPCC_NEW_ORDER_0] = 2;
  p_types_[TPCC_NEW_ORDER_0] = TPCC_NEW_ORDER_0;
  status_[TPCC_NEW_ORDER_0] = READY;

  // piece 1, R warehouse
  inputs_[TPCC_NEW_ORDER_1] = {
      {TPCC_VAR_W_ID, req.input_[TPCC_VAR_W_ID]}
  };
  output_size_[TPCC_NEW_ORDER_1] = 1;
  p_types_[TPCC_NEW_ORDER_1] = TPCC_NEW_ORDER_1;
  status_[TPCC_NEW_ORDER_1] = READY;

  // piece 2, R customer
  inputs_[TPCC_NEW_ORDER_2] = {
      {TPCC_VAR_W_ID, req.input_[TPCC_VAR_W_ID]},
      {TPCC_VAR_D_ID, req.input_[TPCC_VAR_D_ID]},
      {TPCC_VAR_C_ID, req.input_[TPCC_VAR_C_ID]}
  };
  output_size_[TPCC_NEW_ORDER_2] = 3;
  p_types_[TPCC_NEW_ORDER_2] = TPCC_NEW_ORDER_2;
  status_[TPCC_NEW_ORDER_2] = READY;

  bool all_local = true;
  for (int i = 0; i < ol_cnt; i++) {
    Value is_remote((i32) 0);
    if (req.input_[TPCC_VAR_S_W_ID(i)] != req.input_[TPCC_VAR_W_ID]) {
      all_local = false;
      is_remote.set_i32((i32) 1);
    }

    auto pi_ri = TPCC_NEW_ORDER_Ith_INDEX_ITEM(i);
    inputs_[pi_ri] = {
        {TPCC_VAR_I_ID(i), req.input_[TPCC_VAR_I_ID(i)]}
    };
    output_size_[pi_ri] = 3;
    p_types_[pi_ri] = TPCC_NEW_ORDER_RI(i);
    status_[pi_ri] = READY;

    auto pi_rs = TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i);
    inputs_[pi_rs] = {
        {TPCC_VAR_I_ID(i),   req.input_[TPCC_VAR_I_ID(i)]},
        {TPCC_VAR_S_W_ID(i), req.input_[TPCC_VAR_S_W_ID(i)]},
        {TPCC_VAR_S_D_ID(i), req.input_[TPCC_VAR_D_ID]}
    };
    output_size_[pi_rs] = 2;
    p_types_[pi_rs] = TPCC_NEW_ORDER_RS(i);
    status_[pi_rs] = READY;

    auto pi_ws = TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i);
    inputs_[pi_ws] = {
        {TPCC_VAR_I_ID(i),         req.input_[TPCC_VAR_I_ID(i)]},
        {TPCC_VAR_S_W_ID(i),       req.input_[TPCC_VAR_S_W_ID(i)]},
        {TPCC_VAR_OL_QUANTITY(i),  req.input_[TPCC_VAR_OL_QUANTITY(i)]},
        {TPCC_VAR_S_REMOTE_CNT(i), is_remote}
    };
    output_size_[pi_ws] = 0;
    p_types_[pi_ws] = TPCC_NEW_ORDER_WS(i);
    status_[pi_ws] = READY;

    auto pi_wol = TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i);
    inputs_[pi_wol] = {
        {TPCC_VAR_OL_D_ID(i),         req.input_[TPCC_VAR_D_ID]},
        {TPCC_VAR_OL_W_ID(i),         req.input_[TPCC_VAR_W_ID]},
//        {TPCC_VAR_O_ID,               Value((i32) 0)},
        {TPCC_VAR_OL_NUMBER(i),       Value((i32) i)},
        {TPCC_VAR_I_ID(i),            req.input_[TPCC_VAR_I_ID(i)]},
        {TPCC_VAR_S_W_ID(i),          req.input_[TPCC_VAR_S_W_ID(i)]},
        {TPCC_VAR_OL_DELIVER_D(i),    Value(std::string())},
        {TPCC_VAR_OL_QUANTITY(i),     req.input_[TPCC_VAR_OL_QUANTITY(i)]},
//        {TPCC_VAR_OL_AMOUNTS(i),      Value((double) 0.0)},
        {TPCC_VAR_OL_DIST_INFO(i),    Value(std::string())},
    };
    output_size_[pi_wol] = 0;
    p_types_[pi_wol] = TPCC_NEW_ORDER_WOL(i);
    status_[pi_wol] = WAITING;
  }
  // piece 3, W order, depends on piece 0
  inputs_[TPCC_NEW_ORDER_3] = {
//      {TPCC_VAR_O_ID, Value((i32) 0)},
      {TPCC_VAR_D_ID, req.input_[TPCC_VAR_D_ID]},
      {TPCC_VAR_W_ID, req.input_[TPCC_VAR_W_ID]},
      {TPCC_VAR_C_ID, req.input_[TPCC_VAR_C_ID]},
      {TPCC_VAR_O_CARRIER_ID, Value((i32) 0)},
      {TPCC_VAR_OL_CNT, req.input_[TPCC_VAR_OL_CNT]},
      {TPCC_VAR_O_ALL_LOCAL, all_local ? Value((i32) 1) : Value((i32) 0) }
  };
  output_size_[TPCC_NEW_ORDER_3] = 0;
  p_types_[TPCC_NEW_ORDER_3] = TPCC_NEW_ORDER_3;
  status_[TPCC_NEW_ORDER_3] = WAITING;

  // piece 4, W new_order, depends on piece 0
  inputs_[TPCC_NEW_ORDER_4] = {
//      {TPCC_VAR_O_ID, Value((i32) 0)},
      {TPCC_VAR_D_ID, req.input_[TPCC_VAR_D_ID]},
      {TPCC_VAR_W_ID, req.input_[TPCC_VAR_W_ID]},
  };
  output_size_[TPCC_NEW_ORDER_4] = 0;
  p_types_[TPCC_NEW_ORDER_4] = TPCC_NEW_ORDER_4;
  status_[TPCC_NEW_ORDER_4] = WAITING;
}

void TpccChopper::new_order_retry() {
  status_[TPCC_NEW_ORDER_0] = READY;
  status_[TPCC_NEW_ORDER_1] = READY;
  status_[TPCC_NEW_ORDER_2] = READY;
  status_[TPCC_NEW_ORDER_3] = WAITING;
  status_[TPCC_NEW_ORDER_4] = WAITING;
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
  // Ri & W district
  INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_0, TPCC_VAR_W_ID, TPCC_VAR_D_ID)
  SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_0, TPCC_TB_DISTRICT, TPCC_VAR_W_ID)
  BEGIN_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_0, DF_NO) {
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
    output[TPCC_VAR_O_ID] = buf;
    // read d_next_o_id, increment by 1
    buf.set_i32((i32)(buf.get_i32() + 1));
    dtxn->WriteColumn(r,
                      TPCC_COL_DISTRICT_D_NEXT_O_ID,
                      buf,
                      TXN_INSTANT);
    return;
  } END_PIE

  BEGIN_CB(TPCC_NEW_ORDER, TPCC_NEW_ORDER_0)
    TpccChopper* tpcc_ch = (TpccChopper*) ch;
    Value o_id = output[TPCC_VAR_O_ID];
    tpcc_ch->inputs_[TPCC_NEW_ORDER_3][TPCC_VAR_O_ID] = o_id;
    tpcc_ch->inputs_[TPCC_NEW_ORDER_4][TPCC_VAR_O_ID] = o_id;
    tpcc_ch->status_[TPCC_NEW_ORDER_3] = READY;
    tpcc_ch->status_[TPCC_NEW_ORDER_4] = READY;

    for (size_t i = 0; i < tpcc_ch->new_order_dep_.ol_cnt; i++) {
      auto pi = TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i);
      tpcc_ch->inputs_[pi][TPCC_VAR_O_ID] = o_id;
      bool b2 = (tpcc_ch->ws_.find(TPCC_VAR_I_PRICE(i)) != tpcc_ch->ws_.end());
      bool b3 = (tpcc_ch->ws_.find(TPCC_VAR_OL_DIST_INFO(i)) != tpcc_ch->ws_.end());
//      verify(b2 == tpcc_ch->new_order_dep_.piece_items[i]);
//      verify(b3 == tpcc_ch->new_order_dep_.piece_stocks[i]);
      if (b2 && b3)
        tpcc_ch->status_[pi] = READY;
    }

    return true;
  END_CB

  // R warehouse
  INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_1, TPCC_VAR_W_ID)
  SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_1, TPCC_TB_WAREHOUSE, TPCC_VAR_W_ID)
  BEGIN_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_1, DF_NO) {
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

  // R customer //XXX either i or d is ok
  INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_2,
            TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_C_ID)
  SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_2, TPCC_TB_CUSTOMER, TPCC_VAR_W_ID)
  BEGIN_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_2, DF_NO) {
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

  // W order
  INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_3,
            TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_O_ID, TPCC_VAR_C_ID,
            TPCC_VAR_O_CARRIER_ID, TPCC_VAR_OL_CNT, TPCC_VAR_O_ALL_LOCAL)
  SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_3, TPCC_TB_ORDER, TPCC_VAR_W_ID)
  BEGIN_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_3, DF_REAL) {
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

  // W new_order
  INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_4,
            TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_O_ID)
  SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_4, TPCC_TB_NEW_ORDER, TPCC_VAR_W_ID)
  BEGIN_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_4, DF_REAL) {
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

  for (int i = (0); i < (1000); i++) {
    // 1000 is a magical number?
    SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_RI(i),
              TPCC_TB_ITEM, TPCC_VAR_I_ID(i))
    INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_RI(i), TPCC_VAR_I_ID(i))
  }

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


  for (int i = TPCC_NEW_ORDER_RI(0); i < TPCC_NEW_ORDER_RI(1000); i++) {
    // 1000 is a magical number?
    txn_reg_->callbacks_[std::make_pair(TPCC_NEW_ORDER, i)] = \
      [i](TxnChopper *ch, std::map<int32_t, Value> output) -> bool {
      TpccChopper *tpcc_ch = (TpccChopper*) ch;
      auto ol_pi = TPCC_NEW_ORDER_INDEX_ITEM_TO_ORDER_LINE(i);
      auto ol_i = TPCC_NEW_ORDER_INDEX_ITEM_TO_CNT(i);
      auto s_pi = TPCC_NEW_ORDER_INDEX_ITEM_TO_DEFER_STOCK(i);
      tpcc_ch->inputs_[ol_pi][TPCC_VAR_I_PRICE(ol_i)] =
          output[TPCC_VAR_I_PRICE(ol_i)];
      tpcc_ch->inputs_[ol_pi][TPCC_VAR_OL_QUANTITY(ol_i)] =
          tpcc_ch->inputs_[s_pi][TPCC_VAR_OL_QUANTITY(ol_i)];
      tpcc_ch->new_order_dep_.piece_items[ol_i] = true;
      bool b1 = (tpcc_ch->ws_.find(TPCC_VAR_O_ID) != tpcc_ch->ws_.end());
//      verify(b1 == tpcc_ch->new_order_dep_.piece_0_dist);
      bool b3 = (tpcc_ch->ws_.find(TPCC_VAR_OL_DIST_INFO(ol_i)) != tpcc_ch->ws_.end());
//      verify(b3 == tpcc_ch->new_order_dep_.piece_stocks[ol_i]);
      if (b1 && b3) {
        tpcc_ch->status_[ol_pi] = READY;
        return true;
      } else {
        return false;
      }
    };
  }

  for (int i = (0); i < (1000); i++) {
    // 1000 is a magical number?
    SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_RS(i),
              TPCC_TB_STOCK, TPCC_VAR_S_W_ID(i))
    INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_RS(i),
              TPCC_VAR_I_ID(i), TPCC_VAR_S_W_ID(i), TPCC_VAR_S_D_ID(i))
  }

  BEGIN_LOOP_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_RS(0), 1000, DF_NO)
    verify(input.size() == 3);
    Log_debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_RS(I));
    mdb::MultiBlob mb(2);
    mb[0] = input[TPCC_VAR_I_ID(I)].get_blob();
    mb[1] = input[TPCC_VAR_S_W_ID(I)].get_blob();
    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_STOCK), mb, ROW_STOCK);
    verify(r->schema_);
    //i32 s_dist_col = 3 + input[2].get_i32();
    // Ri stock
    // FIXME compress all s_dist_xx into one column
    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_DIST_XX,
                     &output[TPCC_VAR_OL_DIST_INFO(I)],
                     TXN_BYPASS); // 0 ==> s_dist_xx
    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_DATA,
                     &output[TPCC_VAR_S_DATA(I)],
                     TXN_BYPASS); // 1 ==> s_data
    *res = SUCCESS;
    return;
  END_LOOP_PIE

  for (int i = TPCC_NEW_ORDER_RS(0); i < TPCC_NEW_ORDER_RS(1000); i++) {
    // 1000 is a magical number?
    txn_reg_->callbacks_[std::make_pair(TPCC_NEW_ORDER, i)] = \
      [i](TxnChopper *ch, std::map<int32_t, Value> output) -> bool {
      TpccChopper *tpcc_ch = (TpccChopper*) ch;
      auto ol_pi = TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_ORDER_LINE(i);
      auto ol_i = TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_CNT(i);
      tpcc_ch->inputs_[ol_pi][TPCC_VAR_OL_DIST_INFO(ol_i)] =
          output[TPCC_VAR_OL_DIST_INFO(ol_i)];
      tpcc_ch->new_order_dep_.piece_stocks[ol_i] = true;
      bool b1 = (tpcc_ch->ws_.find(TPCC_VAR_O_ID) != tpcc_ch->ws_.end());
//      verify(b1 == tpcc_ch->new_order_dep_.piece_0_dist);
      bool b2 = (tpcc_ch->ws_.find(TPCC_VAR_I_PRICE(ol_i)) != tpcc_ch->ws_.end());
//      verify(b2 == tpcc_ch->new_order_dep_.piece_items[ol_i]);
      if (b1 && b2) {
        tpcc_ch->status_[ol_pi] = READY;
        return true;
      } else {
        return false;
      }
    };
  }


  for (int i = 0; i < (1000); i++) {
    // 1000 is a magical number?
    SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_WS(i),
              TPCC_TB_STOCK, TPCC_VAR_S_W_ID(i))
    INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_WS(i),
              TPCC_VAR_I_ID(i), TPCC_VAR_S_W_ID(i),
              TPCC_VAR_OL_QUANTITY(i), TPCC_VAR_S_REMOTE_CNT(i))
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

  for (int i = 0; i < 1000; i++) {
    // 1000 is a magical number?
    SHARD_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_WOL(i),
              TPCC_TB_ORDER_LINE, TPCC_VAR_W_ID)
    INPUT_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_WOL(i),
              TPCC_VAR_I_ID(i), TPCC_VAR_I_PRICE(i),
              TPCC_VAR_O_ID, TPCC_VAR_S_W_ID(i),
              TPCC_VAR_OL_W_ID(i), TPCC_VAR_OL_D_ID(i),
              TPCC_VAR_OL_DIST_INFO(i), TPCC_VAR_OL_QUANTITY(i),
              TPCC_VAR_OL_NUMBER(i), TPCC_VAR_OL_DELIVER_D(i))
  }

  BEGIN_LOOP_PIE(TPCC_NEW_ORDER, TPCC_NEW_ORDER_WOL(0), 1000, DF_REAL) {
    verify(input.size() == 10);
    Log_debug("TPCC_NEW_ORDER, piece: %d", TPCC_NEW_ORDER_WOL(I));

    mdb::Table *tbl = dtxn->GetTable(TPCC_TB_ORDER_LINE);
    mdb::Row *r = NULL;

    Value amount = Value((double) (input[TPCC_VAR_I_PRICE(I)].get_double() *
              input[TPCC_VAR_OL_QUANTITY(I)].get_i32()));

    CREATE_ROW(tbl->schema(), vector<Value>({
      input[TPCC_VAR_OL_D_ID(I)],
      input[TPCC_VAR_OL_W_ID(I)],
      input[TPCC_VAR_O_ID],
      input[TPCC_VAR_OL_NUMBER(I)],
      input[TPCC_VAR_I_ID(I)],
      input[TPCC_VAR_S_W_ID(I)],
      input[TPCC_VAR_OL_DELIVER_D(I)],
      input[TPCC_VAR_OL_QUANTITY(I)],
      amount,
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
