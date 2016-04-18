
#include "deptran/__dep__.h"
#include "deptran/piece.h"
#include "piece.h"

namespace rococo {

char MICRO_BENCH_TABLE_A[] = "table_a";
char MICRO_BENCH_TABLE_B[] = "table_b";
char MICRO_BENCH_TABLE_C[] = "table_c";
char MICRO_BENCH_TABLE_D[] = "table_d";

void MicroBenchPiece::reg_all() {
    reg_pieces();
}

void MicroBenchPiece::reg_pieces() {
  SHARD_PIE(MICRO_BENCH_W, MICRO_BENCH_W_0,
            MICRO_BENCH_TABLE_A, MICRO_VAR_K_0);
  BEGIN_PIE(MICRO_BENCH_W, MICRO_BENCH_W_0, DF_REAL) {
    verify(cmd.input.size() == 2);
    mdb::MultiBlob buf(1);
    buf[0] = cmd.input[0].get_blob();
    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_A);
    mdb::Row *r = dtxn->Query(tbl, buf);
    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_0]);
  } END_PIE

  SHARD_PIE(MICRO_BENCH_W, MICRO_BENCH_W_1,
            MICRO_BENCH_TABLE_B, MICRO_VAR_K_1);
  BEGIN_PIE(MICRO_BENCH_W, MICRO_BENCH_W_1, DF_REAL) {
    verify(cmd.input.size() == 2);
    mdb::MultiBlob buf(1);
    buf[0] = cmd.input[0].get_blob();
    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_B);
    mdb::Row *r = dtxn->Query(tbl, buf);
    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_1]);
  } END_PIE

  SHARD_PIE(MICRO_BENCH_W, MICRO_BENCH_W_2,
            MICRO_BENCH_TABLE_C, MICRO_VAR_K_2);
  BEGIN_PIE(MICRO_BENCH_W, MICRO_BENCH_W_2, DF_REAL) {
    verify(cmd.input.size() == 2);
    mdb::MultiBlob buf(1);
    buf[0] = cmd.input[0].get_blob();
    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_C);
    mdb::Row *r = dtxn->Query(tbl, buf);
    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_2]);
  } END_PIE

  SHARD_PIE(MICRO_BENCH_W, MICRO_BENCH_W_3,
            MICRO_BENCH_TABLE_D, MICRO_VAR_K_3);
  BEGIN_PIE(MICRO_BENCH_W, MICRO_BENCH_W_3, DF_REAL) {
    verify(cmd.input.size() == 2);
    mdb::MultiBlob buf(1);
    buf[0] = cmd.input[0].get_blob();
    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_D);
    mdb::Row *r = dtxn->Query(tbl, buf);
    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_3]);
  } END_PIE

  SHARD_PIE(MICRO_BENCH_R, MICRO_BENCH_R_0,
            MICRO_BENCH_TABLE_A, MICRO_VAR_K_0);
  BEGIN_PIE(MICRO_BENCH_R, MICRO_BENCH_R_0, DF_REAL) {
    verify(cmd.input.size() == 2);
    mdb::MultiBlob buf(1);
    buf[0] = cmd.input[0].get_blob();
    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_A);
    mdb::Row *r = dtxn->Query(tbl, buf);
    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_0]);
  } END_PIE

  SHARD_PIE(MICRO_BENCH_R, MICRO_BENCH_R_1,
            MICRO_BENCH_TABLE_B, MICRO_VAR_K_1);
  BEGIN_PIE(MICRO_BENCH_R, MICRO_BENCH_R_1, DF_REAL) {
    verify(cmd.input.size() == 2);
    mdb::MultiBlob buf(1);
    buf[0] = cmd.input[0].get_blob();
    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_B);
    mdb::Row *r = dtxn->Query(tbl, buf);
    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_1]);
  } END_PIE

  SHARD_PIE(MICRO_BENCH_R, MICRO_BENCH_R_2,
            MICRO_BENCH_TABLE_C, MICRO_VAR_K_2);
  BEGIN_PIE(MICRO_BENCH_R, MICRO_BENCH_R_2, DF_REAL) {
    verify(cmd.input.size() == 2);
    mdb::MultiBlob buf(1);
    buf[0] = cmd.input[0].get_blob();
    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_C);
    mdb::Row *r = dtxn->Query(tbl, buf);
    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_2]);
  } END_PIE

  SHARD_PIE(MICRO_BENCH_R, MICRO_BENCH_R_3,
            MICRO_BENCH_TABLE_D, MICRO_VAR_K_3);
  BEGIN_PIE(MICRO_BENCH_R, MICRO_BENCH_R_3, DF_REAL) {
    verify(cmd.input.size() == 2);
    mdb::MultiBlob buf(1);
    buf[0] = cmd.input[0].get_blob();
    auto tbl = dtxn->GetTable(MICRO_BENCH_TABLE_D);
    mdb::Row *r = dtxn->Query(tbl, buf);
    dtxn->WriteColumn(r, 1, cmd.input[MICRO_VAR_V_3]);
  } END_PIE
}
} // namespace deptran
