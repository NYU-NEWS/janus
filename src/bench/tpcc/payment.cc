#include "procedure.h"
#include "bench/tpcc_real_dist/sharding.h"
#include "workload.h"

namespace janus {

void TpccProcedure::PaymentInit(TxRequest& req) {
  n_pieces_all_ = 6;
  status_[TPCC_PAYMENT_0] = WAITING;
  status_[TPCC_PAYMENT_1] = WAITING;
  status_[TPCC_PAYMENT_2] = WAITING;
  status_[TPCC_PAYMENT_3] = WAITING;
  status_[TPCC_PAYMENT_4] = WAITING;
  status_[TPCC_PAYMENT_5] = WAITING;
  p_types_[TPCC_PAYMENT_0] = TPCC_PAYMENT_0;
  p_types_[TPCC_PAYMENT_1] = TPCC_PAYMENT_1;
  p_types_[TPCC_PAYMENT_2] = TPCC_PAYMENT_2;
  p_types_[TPCC_PAYMENT_3] = TPCC_PAYMENT_3;
  p_types_[TPCC_PAYMENT_4] = TPCC_PAYMENT_4;
  p_types_[TPCC_PAYMENT_5] = TPCC_PAYMENT_5;
  output_size_[TPCC_PAYMENT_0] = 6;
  // piece 1, Ri district
  output_size_[TPCC_PAYMENT_1] = 6;
  // piece 2, W district
  output_size_[TPCC_PAYMENT_2] = 0;
  n_pieces_dispatchable_ = 0;
  n_pieces_dispatch_acked_ = 0;
  n_pieces_dispatched_ = 0;
  // query by c_last
  if (ws_.count(TPCC_VAR_C_LAST) > 0) {
    Log_debug("payment transaction lookup by customer name");
    // piece 3, R customer, c_last -> c_id
    output_size_[TPCC_PAYMENT_3] = 1;
    // piece 4, set it to waiting
    payment_dep_.piece_last2id = false;
    payment_dep_.piece_ori_last2id = false;
  } else {
    // query by c_id,
    // piece 3, R customer, set it to finish
    status_[TPCC_PAYMENT_3] = OUTPUT_READY;
    // piece 4, set it to ready
    n_pieces_dispatchable_++;
    n_pieces_dispatched_++;
    n_pieces_dispatch_acked_++;
  }

  // piece 4, R & W customer
  output_size_[TPCC_PAYMENT_4] = 15;
  // piece 5, W history (insert), depends on piece 0, 1
  output_size_[TPCC_PAYMENT_5] = 0;
  CheckReady();
}

void TpccProcedure::PaymentRetry() {
  status_[TPCC_PAYMENT_0] = WAITING;
  status_[TPCC_PAYMENT_1] = WAITING;
  status_[TPCC_PAYMENT_2] = WAITING;
  status_[TPCC_PAYMENT_3] = WAITING;
  status_[TPCC_PAYMENT_4] = WAITING;
  status_[TPCC_PAYMENT_5] = WAITING;

  n_pieces_dispatchable_ = 0;
  n_pieces_dispatch_acked_ = 0;
  n_pieces_dispatched_ = 0;
  if (ws_.count(TPCC_VAR_C_LAST) > 0) {
  } else {
    n_pieces_dispatchable_++;
    n_pieces_dispatched_++;
    n_pieces_dispatch_acked_++;
    status_[TPCC_PAYMENT_3] = OUTPUT_READY;
  }
  CheckReady();
}

void TpccWorkload::RegPayment() {
  // piece 0, Ri & W warehouse
  set_op_type(TPCC_PAYMENT, TPCC_PAYMENT_0, READ_REQ);
  RegP(TPCC_PAYMENT,
       TPCC_PAYMENT_0,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_H_AMOUNT,
        TPCC_VAR_C_W_ID, TPCC_VAR_C_D_ID, TPCC_VAR_H_KEY}, // i
       {TPCC_PAYMENT, TPCC_PAYMENT_0, TPCC_VAR_W_NAME, TPCC_VAR_W_STREET_1,
        TPCC_VAR_W_STREET_2, TPCC_VAR_W_CITY, TPCC_VAR_W_STATE,
        TPCC_VAR_W_ZIP}, // o
       {}, // c TODO
       {TPCC_TB_WAREHOUSE, {TPCC_VAR_W_ID}}, // s
       DF_NO,
       PROC {
         verify(cmd.input.size() >= 6);
         Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_0);
         i32 oi = 0;
         mdb::Row* row_warehouse = tx.Query(tx.GetTable(TPCC_TB_WAREHOUSE),
                                            cmd.input[TPCC_VAR_W_ID].get_blob(),
                                            ROW_WAREHOUSE);
         // R warehouse
         output[TPCC_VAR_W_NAME] = Value("");
         tx.ReadColumn(row_warehouse,
                       TPCC_COL_WAREHOUSE_W_NAME,
                       &output[TPCC_VAR_W_NAME],
                       TXN_BYPASS);
         tx.ReadColumn(row_warehouse,
                       TPCC_COL_WAREHOUSE_W_STREET_1,
                       &output[TPCC_VAR_W_STREET_1],
                       TXN_BYPASS);
         tx.ReadColumn(row_warehouse,
                       TPCC_COL_WAREHOUSE_W_STREET_2,
                       &output[TPCC_VAR_W_STREET_2],
                       TXN_BYPASS);
         tx.ReadColumn(row_warehouse,
                       TPCC_COL_WAREHOUSE_W_CITY,
                       &output[TPCC_VAR_W_CITY],
                       TXN_BYPASS);
         tx.ReadColumn(row_warehouse,
                       TPCC_COL_WAREHOUSE_W_STATE,
                       &output[TPCC_VAR_W_STATE],
                       TXN_BYPASS);
         tx.ReadColumn(row_warehouse,
                       TPCC_COL_WAREHOUSE_W_ZIP,
                       &output[TPCC_VAR_W_ZIP],
                       TXN_BYPASS);
         *res = SUCCESS;
       });

  // piece 1, Ri district
  set_op_type(TPCC_PAYMENT, TPCC_PAYMENT_1, READ_REQ);
  RegP(TPCC_PAYMENT, TPCC_PAYMENT_1,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID}, // i
       {TPCC_VAR_D_NAME}, // o
       {}, // c TODO
       {TPCC_TB_DISTRICT, {TPCC_VAR_W_ID}}, DF_NO, PROC {
        verify(cmd.input.size() >= 2);
        Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_1);
        Value buf;
        mdb::MultiBlob mb(2);
        mb[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
        mb[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
        mdb::Row* row_district =
            tx.Query(tx.GetTable(TPCC_TB_DISTRICT), mb, ROW_DISTRICT);
        output[TPCC_VAR_D_NAME] = Value("");

        // R district
        tx.ReadColumn(row_district,
                      TPCC_COL_DISTRICT_D_NAME,
                      &output[TPCC_VAR_D_NAME],
                      TXN_BYPASS);
        tx.ReadColumn(row_district,
                      TPCC_COL_DISTRICT_D_STREET_1,
                      &output[TPCC_VAR_D_STREET_1],
                      TXN_BYPASS);
        tx.ReadColumn(row_district,
                      TPCC_COL_DISTRICT_D_STREET_2,
                      &output[TPCC_VAR_D_STREET_2],
                      TXN_BYPASS);
        tx.ReadColumn(row_district,
                      TPCC_COL_DISTRICT_D_CITY,
                      &output[TPCC_VAR_D_CITY],
                      TXN_BYPASS);
        tx.ReadColumn(row_district,
                      TPCC_COL_DISTRICT_D_STATE,
                      &output[TPCC_VAR_D_STATE],
                      TXN_BYPASS);
        tx.ReadColumn(row_district,
                      TPCC_COL_DISTRICT_D_ZIP,
                      &output[TPCC_VAR_D_ZIP],
                      TXN_BYPASS);

        *res = SUCCESS;
      });

  // piece 1, Ri & W district
  set_op_type(TPCC_PAYMENT, TPCC_PAYMENT_2, WRITE_REQ);
  RegP(TPCC_PAYMENT,
       TPCC_PAYMENT_2,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_H_AMOUNT}, // i
       {}, // o
       {}, // c
       {TPCC_TB_DISTRICT, {TPCC_VAR_W_ID}}, // s
       DF_REAL,
       PROC {
         verify(cmd.input.size() >= 3);
         Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_2);

         Value buf_temp(0.0);
         mdb::Row* row_temp = NULL;
         mdb::MultiBlob mb_temp(2);
         mb_temp[0] = cmd.input[TPCC_VAR_D_ID].get_blob();
         mb_temp[1] = cmd.input[TPCC_VAR_W_ID].get_blob();
         row_temp = tx.Query(tx.GetTable(TPCC_TB_DISTRICT),
                             mb_temp,
                             ROW_DISTRICT_TEMP);
         verify(row_temp->schema_ != nullptr);
         tx.ReadColumn(row_temp,
                       TPCC_COL_DISTRICT_D_YTD,
                       &buf_temp,
                       TXN_BYPASS);
         // W district
         Value buf(0.0);
         buf.set_double(
             buf_temp.get_double() + cmd.input[TPCC_VAR_H_AMOUNT].get_double());
         tx.WriteColumn(row_temp,
                        TPCC_COL_DISTRICT_D_YTD,
                        buf_temp,
                        TXN_DEFERRED);
         *res = SUCCESS;
       });

  set_op_type(TPCC_PAYMENT, TPCC_PAYMENT_5, WRITE_REQ);
  RegP(TPCC_PAYMENT,
       TPCC_PAYMENT_5,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, /**TPCC_VAR_W_NAME, TPCC_VAR_D_NAME,
        TPCC_VAR_C_ID, **/ TPCC_VAR_C_W_ID, TPCC_VAR_C_D_ID, TPCC_VAR_H_KEY,
        TPCC_VAR_H_AMOUNT}, // i
       {}, // o
       {}, // c TODO
       {TPCC_TB_HISTORY, {TPCC_VAR_H_KEY}}, // s
       DF_REAL,
       PROC {
         verify(cmd.input.size() >= 6);
         Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_5);

         mdb::Txn* txn = tx.mdb_txn();
         mdb::Table* tbl = txn->get_table(TPCC_TB_HISTORY);

         // insert history
         mdb::Row* row_history = NULL;

         std::vector<Value> row_data(9);
         row_data[0] = cmd.input[TPCC_VAR_H_KEY];              // h_key
         row_data[1] =
         //    cmd.input[TPCC_VAR_C_ID];               // h_c_id   =>  c_id
             0;               // TODO fix this.
         row_data[2] =
             cmd.input[TPCC_VAR_C_D_ID];             // h_c_d_id =>  c_d_id
         row_data[3] =
             cmd.input[TPCC_VAR_C_W_ID];             // h_c_w_id =>  c_w_id
         row_data[4] =
             cmd.input[TPCC_VAR_D_ID];             // h_d_id   =>  d_id
         row_data[5] =
             cmd.input[TPCC_VAR_W_ID];               // h_d_w_id =>  d_w_id
         row_data[6] = Value(std::to_string(time(NULL)));  // h_date
         row_data[7] =
             cmd.input[TPCC_VAR_H_AMOUNT];           // h_amount =>  h_amount
//         row_data[8] = Value(cmd.input.at(TPCC_VAR_W_NAME).get_str() + "    "
//                                 + cmd.input.at(TPCC_VAR_D_NAME).get_str());
         // d_data => w_name + 4spaces + d_name
         // TODO restore this?
         row_data[8] = Value("   ");

         row_history = tx.CreateRow(tbl->schema(), row_data);
         txn->insert_row(tbl, row_history);
         *res = SUCCESS;
       });

  // piece 2, R customer secondary index, c_last -> c_id
  set_op_type(TPCC_PAYMENT, TPCC_PAYMENT_3, READ_REQ);
  RegP(TPCC_PAYMENT,
       TPCC_PAYMENT_3,
       {TPCC_VAR_C_W_ID, TPCC_VAR_C_D_ID, TPCC_VAR_C_LAST}, // i
       {}, // o
       {}, // c
       {TPCC_TB_CUSTOMER, {TPCC_VAR_C_W_ID}}, // s
       DF_NO,
       PROC {
         verify(cmd.input.size() >= 3);
         Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_3);

         mdb::MultiBlob mbl(3), mbh(3);
         mbl[0] = cmd.input[TPCC_VAR_C_D_ID].get_blob();
         mbh[0] = cmd.input[TPCC_VAR_C_D_ID].get_blob();
         mbl[1] = cmd.input[TPCC_VAR_C_W_ID].get_blob();
         mbh[1] = cmd.input[TPCC_VAR_C_W_ID].get_blob();
         Value c_id_low(std::numeric_limits<i32>::min());
         Value c_id_high(std::numeric_limits<i32>::max());
         mbl[2] = c_id_low.get_blob();
         mbh[2] = c_id_high.get_blob();

         c_last_id_t
             key_low(cmd.input[TPCC_VAR_C_LAST].get_str(), mbl, &C_LAST_SCHEMA);
         c_last_id_t key_high
             (cmd.input[TPCC_VAR_C_LAST].get_str(), mbh, &C_LAST_SCHEMA);
         std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high,
             it_mid;
         bool inc = false, mid_set = false;
         it_low = C_LAST2ID.lower_bound(key_low);
         it_high = C_LAST2ID.upper_bound(key_high);
         int n_c = 0;
         for (it = it_low; it != it_high; it++) {
           n_c++;
           if (mid_set)
             if (inc) {
               it_mid++;
               inc = false;
             } else
               inc = true;
           else {
             mid_set = true;
             it_mid = it;
           }
         }
         Log_debug("w_id: %d, d_id: %d, c_last: %s, num customer: %d",
                   cmd.input[TPCC_VAR_C_W_ID].get_i32(),
                   cmd.input[TPCC_VAR_C_D_ID].get_i32(),
                   cmd.input[TPCC_VAR_C_LAST].get_str().c_str(),
                   n_c);
         verify(mid_set);
         output[TPCC_VAR_C_ID] = Value(it_mid->second);

         *res = SUCCESS;
       });

  // piece 4, R & W customer
  set_op_type(TPCC_PAYMENT, TPCC_PAYMENT_4, WRITE_REQ);
  RegP(TPCC_PAYMENT,
       TPCC_PAYMENT_4,
       {TPCC_VAR_W_ID, TPCC_VAR_D_ID, TPCC_VAR_H_AMOUNT, TPCC_VAR_C_ID,
        TPCC_VAR_C_W_ID, TPCC_VAR_C_D_ID}, // i
       {}, // o
       {}, // c
       {TPCC_TB_CUSTOMER, {TPCC_VAR_C_W_ID}}, // s
       DF_REAL,
       PROC {
         verify(cmd.input.size() >= 6);
         Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_4);
         mdb::Row* r = NULL;
         mdb::MultiBlob mb(3);
         //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
         mb[0] = cmd.input[TPCC_VAR_C_ID].get_blob();
         mb[1] = cmd.input[TPCC_VAR_C_D_ID].get_blob();
         mb[2] = cmd.input[TPCC_VAR_C_W_ID].get_blob();
         // R customer
         r = tx.Query(tx.GetTable(TPCC_TB_CUSTOMER), mb, ROW_CUSTOMER);
         ALock::type_t lock_20_type = ALock::RLOCK;
         if (cmd.input[TPCC_VAR_C_ID].get_i32() % 10 == 0)
           lock_20_type = ALock::WLOCK;

         vector<Value> buf
             ({Value(""), Value(""), Value(""), Value(""), Value(""), Value(""),
               Value(""), Value(""), Value(""), Value(""), Value(""), Value(""),
               Value(""), Value(0.0), Value(0.0), Value("")});
         int oi = 0;
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_FIRST, &buf[0], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_MIDDLE, &buf[1], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_LAST, &buf[2], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_STREET_1, &buf[3], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_STREET_2, &buf[4], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_CITY, &buf[5], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_STATE, &buf[6], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_ZIP, &buf[7], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_PHONE, &buf[8], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_SINCE, &buf[9], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_CREDIT, &buf[10], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_CREDIT_LIM, &buf[11], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_DISCOUNT, &buf[12], TXN_BYPASS);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_BALANCE, &buf[13], TXN_DEFERRED);
         tx.ReadColumn(r,
                       TPCC_COL_CUSTOMER_C_YTD_PAYMENT,
                       &buf[14],
                       TXN_DEFERRED);
         tx.ReadColumn(r, TPCC_COL_CUSTOMER_C_DATA, &buf[15], TXN_DEFERRED);

         // if c_credit == "BC" (bad) 10%
         // here we use c_id to pick up 10% instead of c_credit
         if (cmd.input[TPCC_VAR_C_ID].get_i32() % 10 == 0) {
           Value c_data((to_string(cmd.input[TPCC_VAR_C_ID])
               + to_string(cmd.input[TPCC_VAR_C_D_ID])
               + to_string(cmd.input[TPCC_VAR_C_W_ID])
               + to_string(cmd.input[TPCC_VAR_D_ID])
               + to_string(cmd.input[TPCC_VAR_W_ID])
               + to_string(cmd.input[TPCC_VAR_H_AMOUNT])
               + buf[15].get_str()).substr(0, 500));
           std::vector<mdb::colid_t> col_ids =
               {TPCC_COL_CUSTOMER_C_BALANCE, TPCC_COL_CUSTOMER_C_YTD_PAYMENT,
                TPCC_COL_CUSTOMER_C_DATA};
           std::vector<Value> col_data({Value(buf[13].get_double()
                                                  - cmd.input[TPCC_VAR_H_AMOUNT].get_double()),
                                        Value(buf[14].get_double()
                                                  + cmd.input[TPCC_VAR_H_AMOUNT].get_double()),
                                        c_data});
           tx.WriteColumns(r, col_ids, col_data, TXN_DEFERRED);
         } else {
           std::vector<mdb::colid_t> col_ids
               ({TPCC_COL_CUSTOMER_C_BALANCE, TPCC_COL_CUSTOMER_C_YTD_PAYMENT});
           std::vector<Value> col_data({Value(buf[13].get_double()
                                                  - cmd.input[TPCC_VAR_H_AMOUNT].get_double()),
                                        Value(buf[14].get_double()
                                                  + cmd.input[TPCC_VAR_H_AMOUNT].get_double())});
           tx.WriteColumns(r, col_ids, col_data, TXN_DEFERRED);
         }

         output[TPCC_VAR_C_ID] = cmd.input[TPCC_VAR_D_ID];
         output[TPCC_VAR_C_FIRST] = buf[0];
         output[TPCC_VAR_C_MIDDLE] = buf[1];
         output[TPCC_VAR_C_LAST] = buf[2];
         output[TPCC_VAR_C_STREET_1] = buf[3];
         output[TPCC_VAR_C_STREET_2] = buf[4];
         output[TPCC_VAR_C_CITY] = buf[5];
         output[TPCC_VAR_C_STATE] = buf[6];
         output[TPCC_VAR_C_ZIP] = buf[7];
         output[TPCC_VAR_C_PHONE] = buf[8];
         output[TPCC_VAR_C_SINCE] = buf[9];
         output[TPCC_VAR_C_CREDIT] = buf[10];
         output[TPCC_VAR_C_CREDIT_LIM] = buf[11];
         output[TPCC_VAR_C_DISCOUNT] = buf[12];
         output[TPCC_VAR_C_BALANCE] = Value(
             buf[13].get_double() - cmd.input[TPCC_VAR_H_AMOUNT].get_double());
         *res = SUCCESS;
       });

}

} // namespace janus
