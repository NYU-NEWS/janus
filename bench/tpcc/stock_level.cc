#include "all.h"

namespace rococo {

void TpccPiece::reg_stock_level() {
  BEGIN_PIE(TPCC_STOCK_LEVEL,
          TPCC_STOCK_LEVEL_0, // Ri district
          DF_NO) {
    // ###################################################
    verify(dtxn != nullptr);
    verify(input.size() == 2);
    // ###################################################
    mdb::MultiBlob mb(2);
    //cell_locator_t cl(TPCC_TB_DISTRICT, 2);
    mb[0] = input.at(1).get_blob();
    mb[1] = input.at(0).get_blob();

    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_DISTRICT),
                              mb,
                              ROW_DISTRICT);

    i32 oi = 0;
    dtxn->ReadColumn(r, 10, &output[oi++], TXN_SAFE, TXN_DEFERRED);
                   // output[0] ==> d_next_o_id

    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_STOCK_LEVEL, 0)
    TpccChopper *tpcc_ch = (TpccChopper*) ch;
    verify(output.size() == 1);
    tpcc_ch->inputs_[1][0] = output[0];
    tpcc_ch->status_[1] = READY;
    return true;
  END_CB

  BEGIN_PIE(TPCC_STOCK_LEVEL,
          TPCC_STOCK_LEVEL_1, // Ri order_line
          DF_NO) {
    verify(input.size() == 3);

    mdb::MultiBlob mbl(4), mbh(4);
    mbl[0] = input.at(2).get_blob();
    mbh[0] = input.at(2).get_blob();
    mbl[1] = input.at(1).get_blob();
    mbh[1] = input.at(1).get_blob();
    Value ol_o_id_low(input[0].get_i32() - (i32) 21);
    mbl[2] = ol_o_id_low.get_blob();
    mbh[2] = input[0].get_blob();
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
            header.tid, input[0].get_i32(), input[1].get_i32(), input[2].get_i32());

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
        dtxn->ReadColumn(r, 4, &output[oi++]);
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
    for (std::unordered_set<i32>::iterator s_i_ids_it = s_i_ids.begin();
         s_i_ids_it != s_i_ids.end(); s_i_ids_it++) {
      tpcc_ch->inputs_[2 + i] = map<int32_t, Value>({
                                               {0, Value(*s_i_ids_it)},                      // 0 ==> s_i_id
                                               {1, Value((i32) tpcc_ch->stock_level_dep_.w_id)},      // 1 ==> s_w_id
                                               {2, Value((i32) tpcc_ch->stock_level_dep_.threshold)}  // 2 ==> threshold
                                           });
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
    dtxn->ReadColumn(r, 2, &buf, TXN_SAFE, TXN_DEFERRED);

    if (buf.get_i32() < input[2].get_i32())
        output[output_index++] = Value((i32) 1);
    else
        output[output_index++] = Value((i32) 0);

    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_STOCK_LEVEL, 2)
    return false;
  END_CB
}

} // namespace rococo
