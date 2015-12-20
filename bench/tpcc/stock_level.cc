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

    if ((IS_MODE_RCC || IS_MODE_RO6)) {
        ((RCCDTxn *) dtxn)->kiss(r, 10, false);
    }

    if (RO6_RO_PHASE_1) return;

    i32 oi = 0;
    dtxn->ReadColumn(r, 10, &output[oi++]); // output[0] ==> d_next_o_id

    *res = SUCCESS;
  } END_PIE

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
    if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) {
        ((RCCDTxn *) dtxn)->kiss(r, 2, false);
    }

    if (RO6_RO_PHASE_1) return;

    dtxn->ReadColumn(r, 2, &buf);

    if (buf.get_i32() < input[2].get_i32())
        output[output_index++] = Value((i32) 1);
    else
        output[output_index++] = Value((i32) 0);

    *res = SUCCESS;
  } END_PIE
}

} // namespace rococo
