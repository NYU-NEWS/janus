#include "all.h"

namespace rococo {


void TpccChopper::stock_level_init(TxnRequest &req) {
  /**
   * req.input_
   *  0       ==> w_id
   *  1       ==> d_id
   *  2       ==> threshold
   **/
  n_pieces_all_ = 2;
//  inputs_.resize(n_pieces_all_);
//  output_size_.resize(n_pieces_all_);
//  p_types_.resize(n_pieces_all_);
//  sharding_.resize(n_pieces_all_);
//  status_.resize(n_pieces_all_);

  stock_level_dep_.w_id = req.input_[TPCC_VAR_W_ID].get_i32();
  stock_level_dep_.threshold = req.input_[TPCC_VAR_THRESHOLD].get_i32();

  // piece 0, R district
  inputs_[TPCC_STOCK_LEVEL_0] = {
      {TPCC_VAR_W_ID, req.input_[TPCC_VAR_W_ID]},  // 0    ==> w_id
      {TPCC_VAR_D_ID, req.input_[TPCC_VAR_D_ID]}   // 1    ==> d_id
  };
  output_size_[TPCC_STOCK_LEVEL_0] = 1;
  p_types_[TPCC_STOCK_LEVEL_0] = TPCC_STOCK_LEVEL_0;
  stock_level_shard(TPCC_TB_DISTRICT, req.input_, sharding_[TPCC_STOCK_LEVEL_0]);
  status_[TPCC_STOCK_LEVEL_0] = READY;

  // piece 1, R order_line
  inputs_[TPCC_STOCK_LEVEL_1] = {
      {TPCC_VAR_D_NEXT_O_ID, Value()},        // 0    ==> d_next_o_id, depends on piece 0
      {TPCC_VAR_W_ID, req.input_[TPCC_VAR_W_ID]},         // 1    ==> ol_w_id
      {TPCC_VAR_D_ID, req.input_[TPCC_VAR_D_ID]}          // 2    ==> ol_d_id
  };
  output_size_[TPCC_STOCK_LEVEL_1] = 20 * 15; // 20 orders * 15 order_line per order at most
  p_types_[TPCC_STOCK_LEVEL_1] = TPCC_STOCK_LEVEL_1;
  stock_level_shard(TPCC_TB_ORDER_LINE, req.input_, sharding_[TPCC_STOCK_LEVEL_1]);
  status_[TPCC_STOCK_LEVEL_1] = WAITING;

  // piece 2 - n, R stock init in stock_level_callback
}

void TpccChopper::stock_level_shard(
    const char *tb,
    const map<int32_t, Value> &input,
    uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT ||
      tb == TPCC_TB_ORDER_LINE ||
      tb == TPCC_TB_STOCK)
    // based on w_id
    mv = MultiValue(input.at(TPCC_VAR_W_ID));
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::stock_level_retry() {
  n_pieces_all_ = 2;
//  inputs_.resize(n_pieces_all_);
//  output_size_.resize(n_pieces_all_);
//  p_types_.resize(n_pieces_all_);
//  sharding_.resize(n_pieces_all_);
//  status_.resize(n_pieces_all_);
  status_[TPCC_STOCK_LEVEL_0] = READY;
  status_[TPCC_STOCK_LEVEL_1] = WAITING;
}


void TpccPiece::reg_stock_level() {
  BEGIN_PIE(TPCC_STOCK_LEVEL,
          TPCC_STOCK_LEVEL_0, // Ri district
          DF_NO) {
    verify(dtxn != nullptr);
    verify(input.size() == 2);
    mdb::MultiBlob mb(2);
    //cell_locator_t cl(TPCC_TB_DISTRICT, 2);
    mb[0] = input[TPCC_VAR_D_ID].get_blob();
    mb[1] = input[TPCC_VAR_W_ID].get_blob();

    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_DISTRICT),
                              mb,
                              ROW_DISTRICT);
    dtxn->ReadColumn(r, TPCC_COL_DISTRICT_D_NEXT_O_ID,
                     &output[TPCC_VAR_D_NEXT_O_ID], TXN_DEFERRED);
    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_0)
    TpccChopper *tpcc_ch = (TpccChopper*) ch;
    verify(output.size() == 1);
    tpcc_ch->inputs_[TPCC_STOCK_LEVEL_1][TPCC_VAR_D_NEXT_O_ID] = output[TPCC_VAR_D_NEXT_O_ID];
    tpcc_ch->status_[TPCC_STOCK_LEVEL_1] = READY;
    return true;
  END_CB

  BEGIN_PIE(TPCC_STOCK_LEVEL,
          TPCC_STOCK_LEVEL_1, // Ri order_line
          DF_NO) {
    verify(input.size() == 3);
    mdb::MultiBlob mbl(4), mbh(4);
    mbl[0] = input[TPCC_VAR_D_ID].get_blob();
    mbh[0] = input[TPCC_VAR_D_ID].get_blob();
    mbl[1] = input[TPCC_VAR_W_ID].get_blob();
    mbh[1] = input[TPCC_VAR_W_ID].get_blob();
    Value ol_o_id_low(input[TPCC_VAR_D_NEXT_O_ID].get_i32() - (i32) 21);
    mbl[2] = ol_o_id_low.get_blob();
    mbh[2] = input[TPCC_VAR_D_NEXT_O_ID].get_blob();
    Value ol_number_low(std::numeric_limits<i32>::max()),
            ol_number_high(std::numeric_limits<i32>::min());
    mbl[3] = ol_number_low.get_blob();
    mbh[3] = ol_number_high.get_blob();

    mdb::ResultSet rs = dtxn->QueryIn(dtxn->GetTable(TPCC_TB_ORDER_LINE),
                                      mbl,
                                      mbh,
                                      mdb::ORD_ASC,
                                      header.pid);
    Log_debug("tid: %llx, stock_level: piece 1: d_next_o_id: %d, ol_w_id: %d, ol_d_id: %d",
              header.tid,
              input[TPCC_VAR_D_NEXT_O_ID].get_i32(),
              input[TPCC_VAR_W_ID].get_i32(),
              input[TPCC_VAR_D_ID].get_i32());

    std::vector<mdb::Row *> row_list;
    row_list.reserve(20);

    while (rs.has_next()) {
        row_list.push_back(rs.next());
    }

    verify(row_list.size() != 0);

    std::vector<mdb::column_lock_t> column_locks;
    column_locks.reserve(row_list.size());

    for (int i = 0; i < row_list.size(); i++) {
      dtxn->ReadColumn(row_list[i],
                       TPCC_COL_ORDER_LINE_OL_I_ID,
                       &output[TPCC_VAR_OL_I_ID(i)]);
    }
//    verify(output_size <= 300);
    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_1)
    TpccChopper *tpcc_ch = (TpccChopper*) ch;
    Log_debug("tid %llx: stock_level: outptu_size: %u",
              tpcc_ch->txn_id_, output.size());
    //verify(output_size >= 20 * 5);
//    verify(output_size <= 20 * 15); // TODO fix this verification
    std::unordered_set<i32> s_i_ids;
    for (int i = 0; i < output.size(); i++) {
      s_i_ids.insert(output[TPCC_VAR_OL_I_ID(i)].get_i32());
    }
    tpcc_ch->n_pieces_all_ += s_i_ids.size();
//    inputs_.resize(n_pieces_all_);
//    tpcc_ch->output_size_.resize(tpcc_ch->n_pieces_all_);
//    tpcc_ch->p_types_.resize(tpcc_ch->n_pieces_all_);
//    tpcc_ch->sharding_.resize(tpcc_ch->n_pieces_all_);
//    tpcc_ch->status_.resize(tpcc_ch->n_pieces_all_);
    int i = 0;
    for (auto s_i_ids_it = s_i_ids.begin();
         s_i_ids_it != s_i_ids.end();
         s_i_ids_it++) {
      auto pi = 2 + i;
      tpcc_ch->inputs_[pi] = {
          {TPCC_VAR_OL_I_ID(i), Value(*s_i_ids_it)},
          {TPCC_VAR_W_ID, Value((i32) tpcc_ch->stock_level_dep_.w_id)},
          {TPCC_VAR_THRESHOLD, Value((i32) tpcc_ch->stock_level_dep_.threshold)}
      };
      tpcc_ch->output_size_[pi] = 1;
      tpcc_ch->p_types_[pi] = TPCC_STOCK_LEVEL_RS(i);
      tpcc_ch->stock_level_shard(TPCC_TB_STOCK,
                                 tpcc_ch->inputs_[pi],
                                 tpcc_ch->sharding_[pi]);
      tpcc_ch->status_[pi] = READY;
      i++;
    }
    return true;
  END_CB

  BEGIN_LOOP_PIE(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_RS(0), 1000, DF_NO)
    verify(input.size() == 3);
    Value buf(0);
    mdb::MultiBlob mb(2);
    //cell_locator_t cl(TPCC_TB_STOCK, 2);
    mb[0] = input[TPCC_VAR_OL_I_ID(I)].get_blob();
    mb[1] = input[TPCC_VAR_W_ID].get_blob();

    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_STOCK), mb, ROW_STOCK);
    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_QUANTITY, &buf, TXN_DEFERRED);

    if (buf.get_i32() < input[TPCC_VAR_THRESHOLD].get_i32())
        output[TPCC_VAR_UNKOWN] = Value((i32) 1);
    else
        output[TPCC_VAR_UNKOWN] = Value((i32) 0);

    *res = SUCCESS;
  END_LOOP_PIE
}

} // namespace rococo
