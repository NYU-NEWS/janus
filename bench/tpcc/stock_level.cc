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
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  stock_level_dep_.w_id = req.input_[0].get_i32();
  stock_level_dep_.threshold = req.input_[2].get_i32();

  // piece 0, R district
  inputs_[0] = {
      {TPCC_VAR_W_ID, req.input_[0]},  // 0    ==> w_id
      {TPCC_VAR_D_ID, req.input_[1]}   // 1    ==> d_id
  };
  output_size_[0] = 1;
  p_types_[0] = TPCC_STOCK_LEVEL_0;
  stock_level_shard(TPCC_TB_DISTRICT, req.input_, sharding_[0]);
  status_[0] = READY;

  // piece 1, R order_line
  inputs_[1] = {
      {TPCC_VAR_D_NEXT_O_ID, Value()},        // 0    ==> d_next_o_id, depends on piece 0
      {TPCC_VAR_W_ID, req.input_[0]},         // 1    ==> ol_w_id
      {TPCC_VAR_D_ID, req.input_[1]}          // 2    ==> ol_d_id
  };
  output_size_[1] = 20 * 15; // 20 orders * 15 order_line per order at most
  p_types_[1] = TPCC_STOCK_LEVEL_1;
  stock_level_shard(TPCC_TB_ORDER_LINE, req.input_, sharding_[1]);
  status_[1] = WAITING;

  // piece 2 - n, R stock init in stock_level_callback
}


void TpccChopper::stock_level_shard(
    const char *tb,
    const std::vector<mdb::Value> &input,
    uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT
      || tb == TPCC_TB_ORDER_LINE)
    // based on w_id
    mv = MultiValue(input[0]);
  else if (tb == TPCC_TB_STOCK)
    // based on s_w_id
    mv = MultiValue(input[1]);
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::stock_level_shard(
    const char *tb,
    const map<int32_t, Value> &input,
    uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT
      || tb == TPCC_TB_ORDER_LINE)
    // based on w_id
    mv = MultiValue(input.at(0));
  else if (tb == TPCC_TB_STOCK)
    // based on s_w_id
    mv = MultiValue(input.at(1));
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::stock_level_retry() {
  n_pieces_all_ = 2;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);
  status_[0] = READY;
  status_[1] = WAITING;
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

    i32 oi = 0;
    dtxn->ReadColumn(r, TPCC_COL_DISTRICT_D_NEXT_O_ID,
                     &output[oi++], TXN_DEFERRED);

    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_STOCK_LEVEL, 0)
    TpccChopper *tpcc_ch = (TpccChopper*) ch;
    verify(output.size() == 1);
    tpcc_ch->inputs_[1][TPCC_VAR_D_NEXT_O_ID] = output[0];
    tpcc_ch->status_[1] = READY;
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

    int i = 0;
    i32 oi = 0;
    while (i < row_list.size()) {
        mdb::Row *r = row_list[i++];
        dtxn->ReadColumn(r, TPCC_COL_ORDER_LINE_OL_I_ID, &output[oi++]);
    }
//    verify(output_size <= 300);
    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_STOCK_LEVEL, 1)
    TpccChopper *tpcc_ch = (TpccChopper*) ch;
    Log_debug("tid %llx: stock_level: outptu_size: %u",
              tpcc_ch->txn_id_, output.size());
    //verify(output_size >= 20 * 5);
//    verify(output_size <= 20 * 15); // TODO fix this verification
    std::unordered_set<i32> s_i_ids;
    for (int i = 0; i < output.size(); i++)
      s_i_ids.insert(output[i].get_i32());
    tpcc_ch->n_pieces_all_ += s_i_ids.size();
//    inputs_.resize(n_pieces_all_);
    tpcc_ch->output_size_.resize(tpcc_ch->n_pieces_all_);
    tpcc_ch->p_types_.resize(tpcc_ch->n_pieces_all_);
    tpcc_ch->sharding_.resize(tpcc_ch->n_pieces_all_);
    tpcc_ch->status_.resize(tpcc_ch->n_pieces_all_);
    int i = 0;
    for (auto s_i_ids_it = s_i_ids.begin();
         s_i_ids_it != s_i_ids.end();
         s_i_ids_it++) {
      tpcc_ch->inputs_[2 + i] = {
          {0, Value(*s_i_ids_it)},                      // 0 ==> s_i_id
          {1, Value((i32) tpcc_ch->stock_level_dep_.w_id)},      // 1 ==> s_w_id
          {2, Value((i32) tpcc_ch->stock_level_dep_.threshold)}  // 2 ==> threshold
      };
      tpcc_ch->output_size_[2 + i] = 1;
      tpcc_ch->p_types_[2 + i] = TPCC_STOCK_LEVEL_2;
      tpcc_ch->stock_level_shard(TPCC_TB_STOCK,
                                 tpcc_ch->inputs_[2 + i],
                                 tpcc_ch->sharding_[2 + i]);
      tpcc_ch->status_[2 + i] = READY;
      i++;
    }
    return true;
  END_CB

  BEGIN_PIE(TPCC_STOCK_LEVEL,
            TPCC_STOCK_LEVEL_2, // R stock
            DF_NO) {
    verify(input.size() == 3);
    i32 output_index = 0;
    Value buf(0);
    mdb::MultiBlob mb(2);
    //cell_locator_t cl(TPCC_TB_STOCK, 2);
    mb[0] = input[0].get_blob();
    mb[1] = input[1].get_blob();

    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_STOCK), mb,
                              ROW_STOCK);
    dtxn->ReadColumn(r, TPCC_COL_STOCK_S_QUANTITY, &buf, TXN_DEFERRED);

    if (buf.get_i32() < input[2].get_i32())
        output[output_index++] = Value((i32) 1);
    else
        output[output_index++] = Value((i32) 0);

    *res = SUCCESS;
  } END_PIE
}

} // namespace rococo
