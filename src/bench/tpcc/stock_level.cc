
#include "procedure.h"

namespace janus {


void TpccProcedure::StockLevelInit(TxRequest &req) {

  n_pieces_all_ = 2;
  n_pieces_dispatchable_ = 1;

  stock_level_dep_.w_id = req.input_[TPCC_VAR_W_ID].get_i32();
  stock_level_dep_.threshold = req.input_[TPCC_VAR_THRESHOLD].get_i32();

  // piece 0, R district
  GetWorkspace(TPCC_STOCK_LEVEL_0).keys_ = {
      TPCC_VAR_W_ID,
      TPCC_VAR_D_ID,
  };
  output_size_[TPCC_STOCK_LEVEL_0] = 1;
  p_types_[TPCC_STOCK_LEVEL_0] = TPCC_STOCK_LEVEL_0;
  status_[TPCC_STOCK_LEVEL_0] = DISPATCHABLE;

  // piece 1, R order_line
  GetWorkspace(TPCC_STOCK_LEVEL_1).keys_ = {
      TPCC_VAR_D_NEXT_O_ID,
      TPCC_VAR_W_ID,
      TPCC_VAR_D_ID,
  };
  output_size_[TPCC_STOCK_LEVEL_1] = 20 * 15 + 1; // 20 orders * 15 order_line per order at most
  p_types_[TPCC_STOCK_LEVEL_1] = TPCC_STOCK_LEVEL_1;
  status_[TPCC_STOCK_LEVEL_1] = WAITING;
  // piece 2 - n, R stock init in stock_level_callback
}

void TpccProcedure::StockLevelRetry() {
  n_pieces_all_ = 2;
  n_pieces_dispatchable_ = 1;
  // inputs_.resize(n_pieces_all_);
  // output_size_.resize(n_pieces_all_);
  // p_types_.resize(n_pieces_all_);
  // sharding_.resize(n_pieces_all_);
  //  status_.resize(n_pieces_all_);
  status_[TPCC_STOCK_LEVEL_0] = DISPATCHABLE;
  status_[TPCC_STOCK_LEVEL_1] = WAITING;
}


void TpccWorkload::RegStockLevel() {
  // Ri district
  RegP(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_0,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID}, // i
       {}, // o
       {}, // c TODO
       {TPCC_TB_DISTRICT, {TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
//    verify(input.size() == 2);
         mdb::MultiBlob mb(2);
         //cell_locator_t cl(TPCC_TB_DISTRICT, 2);
         mb[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_W_ID].get_blob();

         auto tbl_district = tx.GetTable(TPCC_TB_DISTRICT);
         mdb::Row *r = tx.Query(tbl_district,
                                   mb,
                                   ROW_DISTRICT);
         tx.ReadColumn(r, TPCC_COL_DISTRICT_D_NEXT_O_ID,
                          &output[TPCC_VAR_D_NEXT_O_ID], TXN_BYPASS);
         *res = SUCCESS;
       }
  );

  // Ri order_line
  RegP(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_1,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_D_NEXT_O_ID}, // i
       {}, // o
       {}, // c
       {TPCC_TB_ORDER_LINE, {TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
//    verify(input.size() == 3);
         mdb::MultiBlob mbl(4), mbh(4);
         mbl[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mbh[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mbl[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mbh[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         Value
             ol_o_id_low(cmd.input[TPCC_VAR_D_NEXT_O_ID].get_i32() - (i32) 21);
         mbl[2] = ol_o_id_low.get_blob();
         mbh[2] = cmd.input[TPCC_VAR_D_NEXT_O_ID].get_blob();
         Value ol_number_low(std::numeric_limits<i32>::max()),
             ol_number_high(std::numeric_limits<i32>::min());
         mbl[3] = ol_number_low.get_blob();
         mbh[3] = ol_number_high.get_blob();

         mdb::ResultSet rs = tx.QueryIn(tx.GetTable(TPCC_TB_ORDER_LINE),
                                           mbl,
                                           mbh,
                                           mdb::ORD_ASC,
                                           cmd.id_);
         Log_debug(
             "tid: %llx, stock_level: piece 1: d_next_o_id: %d, ol_w_id: %d, ol_d_id: %d",
             cmd.root_id_,
             cmd.input[TPCC_VAR_D_NEXT_O_ID].get_i32(),
             cmd.input[TPCC_VAR_W_ID].get_i32(),
             cmd.input[TPCC_VAR_D_ID].get_i32());

         std::vector<mdb::Row *> row_list;
         row_list.reserve(20);

         int i = 0;
         while (i++ < 20 && rs.has_next()) {
           row_list.push_back(rs.next());
         }

//    verify(row_list.size() != 0);

         std::vector<mdb::column_lock_t> column_locks;
         column_locks.reserve(row_list.size());

         for (int i = 0; i < row_list.size(); i++) {
           tx.ReadColumn(row_list[i],
                            TPCC_COL_ORDER_LINE_OL_I_ID,
                            &output[TPCC_VAR_OL_I_ID(i)],
                            TXN_BYPASS);
         }
         output[TPCC_VAR_OL_AMOUNT] = Value((int32_t) row_list.size());
//    verify(output_size <= 300);
         *res = SUCCESS;
       }
  );

  BEGIN_CB(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_1)
    TpccProcedure *tpcc_ch = (TpccProcedure*) ch;
    Log_debug("tid %llx: stock_level: outptu_size: %u",
              tpcc_ch->txn_id_, output.size());
    // verify(output_size >= 20 * 5);
    // verify(output_size <= 20 * 15); // TODO fix this verification
    verify(output.size() == (output[TPCC_VAR_OL_AMOUNT].get_i32() + 1));
    tpcc_ch->n_pieces_all_ += output[TPCC_VAR_OL_AMOUNT].get_i32();
    tpcc_ch->ws_[TPCC_VAR_N_PIECE_ALL] = Value((int32_t)tpcc_ch->n_pieces_all_);

    for (int i = 0; i < output[TPCC_VAR_OL_AMOUNT].get_i32(); i++) {
      auto pi =  TPCC_STOCK_LEVEL_RS(i);
      tpcc_ch->GetWorkspace(pi).keys_ = {
          TPCC_VAR_OL_I_ID(i),
          TPCC_VAR_W_ID,
          TPCC_VAR_THRESHOLD
      };
      tpcc_ch->status_[pi] = DISPATCHABLE;
      tpcc_ch->n_pieces_dispatchable_++;
    }
    return true;
  END_CB


  for (int i = (0); i < (1000); i++) {
    // 1000 is a magical number?
    RegP(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_RS(i),
         {TPCC_VAR_W_ID, TPCC_VAR_OL_I_ID(i), TPCC_VAR_THRESHOLD}, // i
         {}, // o
         {}, // c
         {TPCC_TB_STOCK, {TPCC_VAR_W_ID}}, // s
         DF_NO,
         LPROC {
           Value buf(0);
           mdb::MultiBlob mb(2);
           //cell_locator_t cl(TPCC_TB_STOCK, 2);
           mb[0] = cmd.input[TPCC_VAR_OL_I_ID(i)].get_blob();
           mb[1] = cmd.input[TPCC_VAR_W_ID].get_blob();

           mdb::Row *r = tx.Query(tx.GetTable(TPCC_TB_STOCK), mb, ROW_STOCK);
           tx.ReadColumn(r, TPCC_COL_STOCK_S_QUANTITY, &buf, TXN_BYPASS);

           if (buf.get_i32() < cmd.input[TPCC_VAR_THRESHOLD].get_i32())
             output[TPCC_VAR_UNKOWN] = Value((i32) 1);
           else
             output[TPCC_VAR_UNKOWN] = Value((i32) 0);

           *res = SUCCESS;
         }
    );
  }
}

} // namespace janus
