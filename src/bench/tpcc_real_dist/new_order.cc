#include "procedure.h"
#include "workload.h"

namespace janus {

static uint32_t TXN_TYPE = TPCCD_NEW_ORDER;

void TpccRdProcedure::NewOrderInit(TxRequest &req) {
  NewOrderRetry();
}

void TpccRdProcedure::NewOrderRetry() {
  status_[TPCCD_NEW_ORDER_0] = WAITING;
  status_[TPCCD_NEW_ORDER_1] = WAITING;
  status_[TPCCD_NEW_ORDER_2] = WAITING;
  status_[TPCCD_NEW_ORDER_3] = WAITING;
  status_[TPCCD_NEW_ORDER_4] = WAITING;
  int32_t ol_cnt = ws_[TPCC_VAR_OL_CNT].get_i32();
  n_pieces_all_ = 5 + 4 * ol_cnt;
  n_pieces_dispatchable_ = 0;
  n_pieces_dispatch_acked_ = 0;
  n_pieces_dispatched_ = 0;
  for (size_t i = 0; i < ol_cnt; i++) {
    status_[TPCCD_NEW_ORDER_RI(i)] = WAITING;
    status_[TPCCD_NEW_ORDER_RS(i)] = WAITING;
    status_[TPCCD_NEW_ORDER_WS(i)] = WAITING;
    status_[TPCCD_NEW_ORDER_WOL(i)] = WAITING;
  }
  CheckReady();
}

void TpccRdWorkload::RegNewOrder() {
  // Ri & W district
  RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_0,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID}, // i
       {}, // o
       {}, // TODO c
       {TPCC_TB_DISTRICT, {TPCC_VAR_D_ID, TPCC_VAR_W_ID} }, // s
       DF_NO,
       PROC {
         verify(cmd.input.size() == 2);
         verify(cmd.input[TPCC_VAR_W_ID].get_i32() >= 0);
         mdb::MultiBlob mb(2);
         mb[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         Log_debug("new order d_id: %x w_id: %x",
                   cmd.input[TPCC_VAR_D_ID].get_i32(),
                   cmd.input[TPCC_VAR_W_ID].get_i32());
         mdb::Row *r = tx.Query(tx.GetTable(TPCC_TB_DISTRICT),
                                   mb,
                                   ROW_DISTRICT);
         Value buf(0);
         // R district
         tx.ReadColumn(r,
                          TPCC_COL_DISTRICT_D_TAX,
                          &output[TPCC_VAR_D_TAX],
                          TXN_BYPASS);
         tx.ReadColumn(r,
                          TPCC_COL_DISTRICT_D_NEXT_O_ID,
                          &buf,
                          TXN_BYPASS); // read d_next_o_id
         output[TPCC_VAR_O_ID] = buf;
         // read d_next_o_id, increment by 1
         buf.set_i32((i32) (buf.get_i32() + 1));
         tx.WriteColumn(r,
                           TPCC_COL_DISTRICT_D_NEXT_O_ID,
                           buf,
                           TXN_INSTANT);
         return;
       }
  );

  // R warehouse
  RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_1,
       {TPCC_VAR_W_ID}, // i
       {}, // o
       {}, // TODO c
       {TPCC_TB_DISTRICT, {TPCC_VAR_D_ID, TPCC_VAR_W_ID}},
       DF_NO,
       PROC {
         verify(cmd.input.size() == 1);
         Log::debug("TPCCD_NEW_ORDER, piece: %d", TPCCD_NEW_ORDER_1);
         mdb::Row
             *row_warehouse = tx.Query(tx.GetTable(TPCC_TB_WAREHOUSE),
                                          cmd.input[TPCC_VAR_W_ID].get_blob(),
                                          ROW_WAREHOUSE);
         // R warehouse
         tx.ReadColumn(row_warehouse, TPCC_COL_WAREHOUSE_W_TAX,
                          &output[TPCC_VAR_W_TAX], TXN_BYPASS); // read w_tax
         return;
       }
  );

  // R customer //XXX either i or d is ok
  RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_2,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_C_ID}, // i
       {}, // o
       {}, // c
       {TPCC_TB_CUSTOMER, {TPCC_VAR_D_ID, TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
         verify(cmd.input.size() == 3);
         Log::debug("TPCCD_NEW_ORDER, piece: %d", TPCCD_NEW_ORDER_2);

         mdb::MultiBlob mb(3);
         mb[0] = cmd.input[TPCC_VAR_C_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb[2] = cmd.input[TPCC_VAR_W_ID].get_blob();
         auto table = tx.GetTable(TPCC_TB_CUSTOMER);
         mdb::Row *row_customer = tx.Query(table, mb, ROW_CUSTOMER);
         // R customer
         tx.ReadColumn(row_customer, TPCC_COL_CUSTOMER_C_LAST,
                          &output[TPCC_VAR_C_LAST], TXN_BYPASS);
         tx.ReadColumn(row_customer, TPCC_COL_CUSTOMER_C_CREDIT,
                          &output[TPCC_VAR_C_CREDIT], TXN_BYPASS);
         tx.ReadColumn(row_customer, TPCC_COL_CUSTOMER_C_DISCOUNT,
                          &output[TPCC_VAR_C_DISCOUNT], TXN_BYPASS);

         return;
       }
  );

  // W order
  RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_3,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID,
        TPCC_VAR_O_ID, TPCC_VAR_C_ID,
        TPCC_VAR_O_CARRIER_ID, TPCC_VAR_OL_CNT,
        TPCC_VAR_O_ALL_LOCAL}, // i
       {}, // o
       {}, // c
       {TPCC_TB_ORDER, {TPCC_VAR_D_ID, TPCC_VAR_W_ID}}, // s
       DF_REAL,
       PROC {
         verify(cmd.input.size() == 7);
         Log::debug("TPCCD_NEW_ORDER, piece: %d", TPCCD_NEW_ORDER_3);
         i32 oi = 0;
         mdb::Table *tbl = tx.GetTable(TPCC_TB_ORDER);

         mdb::MultiBlob mb(3);
         mb[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mb[2] = cmd.input[TPCC_VAR_C_ID].get_blob();

         mdb::Row *r = tx.Query(tx.GetTable(TPCC_TB_ORDER_C_ID_SECONDARY),
                                   mb,
                                   ROW_ORDER_SEC);
         verify(r);
         verify(r->schema_);

         // W order
         std::vector<Value> row_data = {
             cmd.input[TPCC_VAR_D_ID],
             cmd.input[TPCC_VAR_W_ID],
             cmd.input[TPCC_VAR_O_ID],
             cmd.input[TPCC_VAR_C_ID],
             Value(std::to_string(time(NULL))),  // o_entry_d
             cmd.input[TPCC_VAR_O_CARRIER_ID],
             cmd.input[TPCC_VAR_OL_CNT],
             cmd.input[TPCC_VAR_O_ALL_LOCAL]
         };
         CREATE_ROW(tbl->schema(), row_data);
         verify(r->schema_);
         tx.InsertRow(tbl, r);

         r = tx.Query(tx.GetTable(TPCC_TB_ORDER_C_ID_SECONDARY),
                         mb,
                         ROW_ORDER_SEC);
         tx.WriteColumn(r, 3, cmd.input[TPCC_VAR_W_ID], TXN_DEFERRED);
         return;
       }
  );

  // W new_order
  RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_4,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_O_ID}, // i
       {}, // o
       {}, // c
       {TPCC_TB_NEW_ORDER, {TPCC_VAR_D_ID, TPCC_VAR_W_ID}}, // s
       DF_REAL,
       PROC {
         verify(cmd.input.size() == 3);
         Log_debug("TPCCD_NEW_ORDER, piece: %d", TPCCD_NEW_ORDER_4);

         mdb::Table *tbl = tx.GetTable(TPCC_TB_NEW_ORDER);
         mdb::Row *r = NULL;

         // W new_order
         std::vector<Value> row_data(
             {
                 cmd.input[TPCC_VAR_D_ID],   // o_d_id
                 cmd.input[TPCC_VAR_W_ID],   // o_w_id
                 cmd.input[TPCC_VAR_O_ID],   // o_id
             });

         CREATE_ROW(tbl->schema(), row_data);

         tx.InsertRow(tbl, r);
         *res = SUCCESS;
         return;
       }
  );

  for (int i = (0); i < (1000); i++) {
    // 1000 is a magical number?
    RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_RI(i),
         {TPCC_VAR_I_ID(i)}, // i
         {}, // o
         {}, // c TODO
         {TPCC_TB_ITEM, {TPCC_VAR_I_ID(i), TPCC_VAR_S_W_ID(i)}}, // s
         DF_NO,
         LPROC {
           verify(cmd.input.size() == 1);
           Log_debug("TPCCD_NEW_ORDER, piece: %d", TPCCD_NEW_ORDER_RI(i));
           mdb::Row *r = tx.Query(tx.GetTable(TPCC_TB_ITEM),
                                     cmd.input[TPCC_VAR_I_ID(i)].get_blob(),
                                     ROW_ITEM);
           // Ri item
           tx.ReadColumn(r,
                            TPCC_COL_ITEM_I_NAME,
                            &output[TPCC_VAR_I_NAME(i)],
                            TXN_BYPASS);
           tx.ReadColumn(r,
                            TPCC_COL_ITEM_I_PRICE,
                            &output[TPCC_VAR_I_PRICE(i)],
                            TXN_BYPASS);
           tx.ReadColumn(r,
                            TPCC_COL_ITEM_I_DATA,
                            &output[TPCC_VAR_I_DATA(i)],
                            TXN_BYPASS);

           *res = SUCCESS;
           return;
         }
    );

    RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_RS(i),
         {TPCC_VAR_D_ID, TPCC_VAR_I_ID(i), TPCC_VAR_S_W_ID(i)}, // i
         {}, // o
         {}, // c
         {TPCC_TB_STOCK, {TPCC_VAR_I_ID(i), TPCC_VAR_S_W_ID(i)}}, // s
         DF_NO,
         LPROC {
           verify(cmd.input.size() == 3);
           Log_debug("TPCCD_NEW_ORDER, piece: %d", TPCCD_NEW_ORDER_RS(i));
           mdb::MultiBlob mb(2);
           mb[0] = cmd.input[TPCC_VAR_I_ID(i)].get_blob();
           mb[1] = cmd.input[TPCC_VAR_S_W_ID(i)].get_blob();
           int32_t w_id = cmd.input[TPCC_VAR_S_W_ID(i)].get_i32();
           int32_t i_id = cmd.input[TPCC_VAR_I_ID(i)].get_i32();
           Log_debug("new order read stock. item_id: %x, s_w_id: %x",
                     i_id,
                     w_id);
           auto tbl_stock = tx.GetTable(TPCC_TB_STOCK);
           mdb::Row *r = tx.Query(tbl_stock, mb, ROW_STOCK);
           verify(r->schema_);
           //i32 s_dist_col = 3 + input[2].get_i32();
           // Ri stock
           // FIXME compress all s_dist_xx into one column
           tx.ReadColumn(r, TPCC_COL_STOCK_S_DIST_XX,
                            &output[TPCC_VAR_OL_DIST_INFO(i)],
                            TXN_BYPASS); // 0 ==> s_dist_xx
           tx.ReadColumn(r, TPCC_COL_STOCK_S_DATA,
                            &output[TPCC_VAR_S_DATA(i)],
                            TXN_BYPASS); // 1 ==> s_data
           *res = SUCCESS;
           return;
         }
    );

    RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_WS(i),
         {TPCC_VAR_I_ID(i), TPCC_VAR_S_W_ID(i),
          TPCC_VAR_OL_QUANTITY(i), TPCC_VAR_S_REMOTE_CNT(i)}, // i
         {}, // o
         {}, // c
         {TPCC_TB_STOCK, {TPCC_VAR_I_ID(i), TPCC_VAR_S_W_ID(i)}}, // s
         DF_REAL,
         LPROC {
           verify(cmd.input.size() == 4);
           Log_debug("TPCCD_NEW_ORDER, piece: %d", TPCCD_NEW_ORDER_WS(i));
           mdb::Row *r = NULL;
           mdb::MultiBlob mb(2);
           mb[0] = cmd.input[TPCC_VAR_I_ID(i)].get_blob();
           mb[1] = cmd.input[TPCC_VAR_S_W_ID(i)].get_blob();

           r = tx.Query(tx.GetTable(TPCC_TB_STOCK), mb, ROW_STOCK_TEMP);
           verify(r->schema_);
           // Ri stock
           Value buf(0);
           tx.ReadColumn(r, TPCC_COL_STOCK_S_QUANTITY, &buf, TXN_DEFERRED);
           int32_t new_ol_quantity = buf.get_i32() -
               cmd.input[TPCC_VAR_OL_QUANTITY(i)].get_i32();

           tx.ReadColumn(r, TPCC_COL_STOCK_S_YTD, &buf, TXN_DEFERRED);
           Value new_s_ytd(buf.get_i32() +
               cmd.input[TPCC_VAR_OL_QUANTITY(i)].get_i32());

           tx.ReadColumn(r, TPCC_COL_STOCK_S_ORDER_CNT, &buf, TXN_DEFERRED);
           Value new_s_order_cnt((i32)(buf.get_i32() + 1));

           tx.ReadColumn(r, TPCC_COL_STOCK_S_REMOTE_CNT, &buf, TXN_DEFERRED);
           Value new_s_remote_cnt(buf.get_i32() +
               cmd.input[TPCC_VAR_S_REMOTE_CNT(i)].get_i32());

           if (new_ol_quantity < 10)
             new_ol_quantity += 91;
           Value new_ol_quantity_value(new_ol_quantity);

           tx.WriteColumns(r,
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
                              },
                              TXN_DEFERRED);
           *res = SUCCESS;
           return;
         }
    );

    RegP(TPCCD_NEW_ORDER, TPCCD_NEW_ORDER_WOL(i),
         {TPCC_VAR_I_ID(i), TPCC_VAR_I_PRICE(i),
          TPCC_VAR_O_ID, TPCC_VAR_S_W_ID(i),
          TPCC_VAR_W_ID, TPCC_VAR_D_ID,
          TPCC_VAR_OL_DIST_INFO(i), TPCC_VAR_OL_QUANTITY(i),
          TPCC_VAR_OL_NUMBER(i), TPCC_VAR_OL_DELIVER_D(i)}, // i
         {}, // o
         {}, // c TODO
         {TPCC_TB_ORDER_LINE, {TPCC_VAR_D_ID, TPCC_VAR_W_ID}}, // s
         DF_REAL,
         LPROC {
           verify(cmd.input.size() == 10);
           Log_debug("TPCCD_NEW_ORDER, piece: %d",
                     TPCCD_NEW_ORDER_WOL(i));

           mdb::Table *tbl = tx.GetTable(TPCC_TB_ORDER_LINE);
           mdb::Row *r = NULL;

           Value amount = Value((double) (
               cmd.input[TPCC_VAR_I_PRICE(i)].get_double() *
               cmd.input[TPCC_VAR_OL_QUANTITY(i)].get_i32()));

           vector<Value> row_data = {
                   cmd.input[TPCC_VAR_D_ID],
                   cmd.input[TPCC_VAR_W_ID],
                   cmd.input[TPCC_VAR_O_ID],
                   cmd.input[TPCC_VAR_OL_NUMBER(i)],
                   cmd.input[TPCC_VAR_I_ID(i)],
                   cmd.input[TPCC_VAR_S_W_ID(i)],
                   cmd.input[TPCC_VAR_OL_DELIVER_D(i)],
                   cmd.input[TPCC_VAR_OL_QUANTITY(i)],
                   amount,
                   cmd.input[TPCC_VAR_OL_DIST_INFO(i)]
           };
           CREATE_ROW(tbl->schema(), row_data);

           tx.InsertRow(tbl, r);
           *res = SUCCESS;
           return;

         }
    );
  }
}
} // namespace janus
