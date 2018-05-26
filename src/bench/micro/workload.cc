
#include "deptran/__dep__.h"
#include "deptran/workload.h"
#include "workload.h"

namespace janus {

char MICRO_BENCH_TABLE_A[] = "table_a";
char MICRO_BENCH_TABLE_B[] = "table_b";
char MICRO_BENCH_TABLE_C[] = "table_c";
char MICRO_BENCH_TABLE_D[] = "table_d";

void MicroWorkload::GetReadReq(TxRequest *req, uint32_t cid) const {
  req->tx_type_ = MICRO_BENCH_R;
  req->input_[0] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_a_ - 1));
  req->input_[1] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_b_ - 1));
  req->input_[2] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_c_ - 1));
  req->input_[3] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_d_ - 1));
}

void MicroWorkload::GetWriteReq(TxRequest *req, uint32_t cid) const {
  req->tx_type_ = MICRO_BENCH_W;
  req->input_[0] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_a_ - 1));
  req->input_[1] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_b_ - 1));
  req->input_[2] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_c_ - 1));
  req->input_[3] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_d_ - 1));
  req->input_[4] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[5] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[6] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[7] = Value((i64) RandomGenerator::rand(0, 1000));
}

void MicroWorkload::GetTxRequest(TxRequest* req, uint32_t cid) {
  req->n_try_ = n_try_;
  if (txn_weight_.size() != 2)
    GetReadReq(req, cid);
  else
    switch (RandomGenerator::weighted_select(txn_weight_)) {
      case 0: // read
        GetReadReq(req, cid);
        break;
      case 1: // write
        GetWriteReq(req, cid);
        break;
    }
}

void MicroWorkload::RegisterPrecedures() {
  verify(0);
  RegP(MICRO_BENCH_W, MICRO_BENCH_W_0,
       {}, // i TODO
       {}, // o TODO
       {}, // c TODO
       {MICRO_BENCH_TABLE_A, {MICRO_VAR_K_0}}, // s
       DF_REAL,
       PROC {
         verify(cmd.input.size() == 2);
         mdb::MultiBlob buf(1);
         buf[0] = cmd.input[0].get_blob();
         auto tbl = tx.GetTable(MICRO_BENCH_TABLE_A);
         mdb::Row *r = tx.Query(tbl, buf);
         tx.WriteColumn(r, 1, cmd.input[MICRO_VAR_V_0]);
       }
  );

  RegP(MICRO_BENCH_W, MICRO_BENCH_W_2,
       {}, // i TODO
       {}, // o TODO
       {}, // c TODO
       {MICRO_BENCH_TABLE_C, {MICRO_VAR_K_2}}, // s
       DF_REAL,
       PROC {
         verify(cmd.input.size() == 2);
         mdb::MultiBlob buf(1);
         buf[0] = cmd.input[0].get_blob();
         auto tbl = tx.GetTable(MICRO_BENCH_TABLE_C);
         mdb::Row *r = tx.Query(tbl, buf);
         tx.WriteColumn(r, 1, cmd.input[MICRO_VAR_V_2]);
       }
  );



//  SHARD_PIE(MICRO_BENCH_W, MICRO_BENCH_W_3,
//            MICRO_BENCH_TABLE_D, MICRO_VAR_K_3);
//  BEGIN_PIE(MICRO_BENCH_W, MICRO_BENCH_W_3, DF_REAL) {
//    verify(cmd.input.size() == 2);
//    mdb::MultiBlob buf(1);
//    buf[0] = cmd.input[0].get_blob();
//    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_D);
//    mdb::Row *r = dtxn->Query(tbl, buf);
//    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_3]);
//  } END_PIE
//
//  SHARD_PIE(MICRO_BENCH_R, MICRO_BENCH_R_0,
//            MICRO_BENCH_TABLE_A, MICRO_VAR_K_0);
//  BEGIN_PIE(MICRO_BENCH_R, MICRO_BENCH_R_0, DF_REAL) {
//    verify(cmd.input.size() == 2);
//    mdb::MultiBlob buf(1);
//    buf[0] = cmd.input[0].get_blob();
//    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_A);
//    mdb::Row *r = dtxn->Query(tbl, buf);
//    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_0]);
//  } END_PIE
//
//  SHARD_PIE(MICRO_BENCH_R, MICRO_BENCH_R_1,
//            MICRO_BENCH_TABLE_B, MICRO_VAR_K_1);
//  BEGIN_PIE(MICRO_BENCH_R, MICRO_BENCH_R_1, DF_REAL) {
//    verify(cmd.input.size() == 2);
//    mdb::MultiBlob buf(1);
//    buf[0] = cmd.input[0].get_blob();
//    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_B);
//    mdb::Row *r = dtxn->Query(tbl, buf);
//    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_1]);
//  } END_PIE
//
//  SHARD_PIE(MICRO_BENCH_R, MICRO_BENCH_R_2,
//            MICRO_BENCH_TABLE_C, MICRO_VAR_K_2);
//  BEGIN_PIE(MICRO_BENCH_R, MICRO_BENCH_R_2, DF_REAL) {
//    verify(cmd.input.size() == 2);
//    mdb::MultiBlob buf(1);
//    buf[0] = cmd.input[0].get_blob();
//    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_C);
//    mdb::Row *r = dtxn->Query(tbl, buf);
//    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_2]);
//  } END_PIE
//
//  SHARD_PIE(MICRO_BENCH_R, MICRO_BENCH_R_3,
//            MICRO_BENCH_TABLE_D, MICRO_VAR_K_3);
//  BEGIN_PIE(MICRO_BENCH_R, MICRO_BENCH_R_3, DF_REAL) {
//    verify(cmd.input.size() == 2);
//    mdb::MultiBlob buf(1);
//    buf[0] = cmd.input[0].get_blob();
//    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_D);
//    mdb::Row *r = dtxn->Query(tbl, buf);
//    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_3]);
//  } END_PIE
}
} // namespace janus
