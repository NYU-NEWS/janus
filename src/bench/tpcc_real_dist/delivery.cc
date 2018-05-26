#include "deptran/__dep__.h"
#include "procedure.h"
#include "workload.h"

namespace janus {

void TpccRdProcedure::DeliveryInit(TxRequest &req) {
  n_pieces_all_ = 4;

  // piece 0, Ri & W new_order
  output_size_[TPCC_DELIVERY_0] = 1;
  // piece 1, Ri & W order
  output_size_[TPCC_DELIVERY_1] = 1;
  // piece 2, Ri & W order_line
  output_size_[TPCC_DELIVERY_2] = 1;
  // piece 3, W customer
  output_size_[TPCC_DELIVERY_3] = 0;

  p_types_[TPCC_DELIVERY_0] = TPCC_DELIVERY_0;
  p_types_[TPCC_DELIVERY_1] = TPCC_DELIVERY_1;
  p_types_[TPCC_DELIVERY_2] = TPCC_DELIVERY_2;
  p_types_[TPCC_DELIVERY_3] = TPCC_DELIVERY_3;

  status_[TPCC_DELIVERY_0] = WAITING;
  status_[TPCC_DELIVERY_1] = WAITING;
  status_[TPCC_DELIVERY_2] = WAITING;
  status_[TPCC_DELIVERY_3] = WAITING;
  CheckReady();
}

void TpccRdProcedure::DeliveryRetry() {
  status_[TPCC_DELIVERY_0] = WAITING;
  status_[TPCC_DELIVERY_1] = WAITING;
  status_[TPCC_DELIVERY_2] = WAITING;
  status_[TPCC_DELIVERY_3] = WAITING;
  CheckReady();
}


void TpccRdWorkload::RegDelivery() {
  // Ri & W new_order
  RegP(TPCC_DELIVERY, TPCC_DELIVERY_0,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID}, // i
       {}, // o
       {}, // TODO c
       {TPCC_TB_NEW_ORDER, {TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
         // this is a little bit tricky, the first half will do most of the job,
         // removing the row from the table, but it won't actually release the
         // resource. And the bottom half is in charge of release the resource,
         // including the vertex entry

         Log_debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_0);
         verify(cmd.input.size() == 2);
         Value buf;
         //cell_locator_t cl(TPCC_TB_NEW_ORDER, 3);
         mdb::Row *r = NULL;
         mdb::Table *tbl = tx.GetTable(TPCC_TB_NEW_ORDER);

         mdb::MultiBlob mbl(3), mbh(3);
         mbl[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mbh[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mbl[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mbh[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         Value no_o_id_low(std::numeric_limits<i32>::min()),
             no_o_id_high(std::numeric_limits<i32>::max());
         mbl[2] = no_o_id_low.get_blob();
         mbh[2] = no_o_id_high.get_blob();

         mdb::ResultSet rs = tx.QueryIn(tbl,
                                           mbl,
                                           mbh,
                                           mdb::ORD_ASC,
                                           cmd.id_);
         if (rs.has_next()) {
           r = rs.next();
//      TPL_KISS_ROW(r);
           tx.ReadColumn(r, TPCC_COL_NEW_ORDER_NO_W_ID, &buf);
           output[TPCC_VAR_O_ID] = buf;
         } else {
//      verify(0);
//      TPL_KISS_NONE;
           output[TPCC_VAR_O_ID] = Value((i32) -1);
         }
         // TODO FIXME
//    if (r) {
//      mdb::Txn *txn = tx.mdb_txn();
//      txn->remove_row(tbl, r);
//    }

         *res = SUCCESS;
         return;
       }
  );

  // Ri & W order
  RegP(TPCC_DELIVERY, TPCC_DELIVERY_1,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID,
        TPCC_VAR_O_ID, TPCC_VAR_O_CARRIER_ID}, // i
       {}, // o
       {}, // TODO c
       {TPCC_TB_ORDER, {TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
         Log_debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_1);
         verify(cmd.input.size() == 4);
         mdb::Txn *txn = tx.mdb_txn_;
         mdb::MultiBlob mb(3);
         //cell_locator_t cl(TPCC_TB_ORDER, 3);
         mb[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         mb[2] = cmd.input[TPCC_VAR_O_ID].get_blob();
         //Log::debug("Delivery: o_d_id: %d, o_w_id: %d, o_id: %d, hash: %u", input[2].get_i32(), input[1].get_i32(), input[0].get_i32(), mdb::MultiBlob::hash()(cl.primary_key));
         mdb::Row *row_order =
             tx.Query(txn->get_table(TPCC_TB_ORDER), mb, ROW_ORDER);
         tx.ReadColumn(row_order,
                          TPCC_COL_ORDER_O_C_ID,
                          &output[TPCC_VAR_C_ID],
                          TXN_BYPASS); // read o_c_id
         tx.WriteColumn(row_order,
                           TPCC_COL_ORDER_O_CARRIER_ID,
                           cmd.input[TPCC_VAR_O_CARRIER_ID],
                           TXN_INSTANT); // write o_carrier_id
         return;
       }
  );

  // Ri & W order_line
  RegP(TPCC_DELIVERY, TPCC_DELIVERY_2,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_O_ID}, // i
       {}, // o
       {}, // c
       {TPCC_TB_ORDER_LINE, {TPCC_VAR_W_ID} }, // s
       DF_NO,
       PROC {
         Log_debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_2);
         verify(cmd.input.size() == 3);
         //        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
         mdb::MultiBlob mbl(4), mbh(4);
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
                                           mdb::ORD_ASC,
                                           cmd.id_);
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
         double ol_amount_buf = 0.0;

         while (i < row_list.size()) {
           r = row_list[i++];
           Value buf(0.0);
           tx.ReadColumn(r, TPCC_COL_ORDER_LINE_OL_AMOUNT,
                            &buf, TXN_INSTANT); // read ol_amount
           ol_amount_buf += buf.get_double();
           tx.WriteColumn(r, TPCC_COL_ORDER_LINE_OL_DELIVERY_D,
                             Value(std::to_string(time(NULL))),
                             TXN_INSTANT);
         }
         output[TPCC_VAR_OL_AMOUNT] = Value(ol_amount_buf);

         *res = SUCCESS;
       }
  );

  // W customer
  RegP(TPCC_DELIVERY, TPCC_DELIVERY_3,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID,
        TPCC_VAR_C_ID, TPCC_VAR_OL_AMOUNT}, // i
       {}, // o
       {}, // TODO, c
       {TPCC_TB_CUSTOMER, {TPCC_VAR_W_ID}}, // s
       DF_REAL,
       PROC {
         Log_debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_3);
         verify(cmd.input.size() == 4);
         mdb::Row *row_customer = NULL;
         mdb::MultiBlob mb(3);
         //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
         mb[0] = cmd.input[TPCC_VAR_C_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb[2] = cmd.input[TPCC_VAR_W_ID].get_blob();

         row_customer =
             tx.Query(tx.GetTable(TPCC_TB_CUSTOMER), mb, ROW_CUSTOMER);
         Value buf(0.0);
         tx.ReadColumn(row_customer,
                          TPCC_COL_CUSTOMER_C_BALANCE,
                          &buf,
                          TXN_DEFERRED);
         buf.set_double(
             buf.get_double() + cmd.input[TPCC_VAR_OL_AMOUNT].get_double());
         tx.WriteColumn(row_customer, TPCC_COL_CUSTOMER_C_BALANCE,
                           buf, TXN_DEFERRED);
         tx.ReadColumn(row_customer, TPCC_COL_CUSTOMER_C_DELIVERY_CNT,
                          &buf, TXN_BYPASS);
         buf.set_i32(buf.get_i32() + (i32) 1);
         tx.WriteColumn(row_customer, TPCC_COL_CUSTOMER_C_DELIVERY_CNT,
                           buf, TXN_DEFERRED);
         *res = SUCCESS;
       }
  );
}

} // namespace janus
