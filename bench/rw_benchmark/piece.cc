#include "deptran/__dep__.h"
#include "../tpcc/piece.h"
#include "piece.h"

namespace rococo {
  char RW_BENCHMARK_TABLE[] = "history";

  void RWPiece::reg_all() {
      reg_pieces();
  }

  void RWPiece::reg_pieces() {

    SHARD_PIE(RW_BENCHMARK_R_TXN, RW_BENCHMARK_R_TXN_0,
              TPCC_TB_HISTORY, TPCC_VAR_H_KEY)
    BEGIN_PIE(RW_BENCHMARK_R_TXN, RW_BENCHMARK_R_TXN_0, DF_NO) {
      mdb::MultiBlob buf(1);
      Value result(0);
      verify(cmd.input.size()==1);
      auto id = cmd.input[0].get_i64();
      buf[0] = cmd.input[0].get_blob();
      auto tbl = dtxn->GetTable(RW_BENCHMARK_TABLE);
      auto row = dtxn->Query(tbl, buf);
      dtxn->ReadColumn(row, 1, &result, TXN_BYPASS);
      output[0] = result;
      *res = SUCCESS;
      return;
    } END_PIE

    SHARD_PIE(RW_BENCHMARK_W_TXN, RW_BENCHMARK_W_TXN_0,
              TPCC_TB_HISTORY, TPCC_VAR_H_KEY)
    BEGIN_PIE(RW_BENCHMARK_W_TXN, RW_BENCHMARK_W_TXN_0, DF_REAL) {
      mdb::MultiBlob buf(1);
      Value result(0);
      verify(cmd.input.size() == 1);
      auto id = cmd.input[0].get_i64();
      buf[0] = cmd.input[0].get_blob();
      auto tbl = dtxn->GetTable(RW_BENCHMARK_TABLE);
      auto row = dtxn->Query(tbl, buf);
      dtxn->ReadColumn(row, 1, &result, TXN_BYPASS);
      result.set_i32(result.get_i32()+1);
      dtxn->WriteColumn(row, 1, result, TXN_DEFERRED);
      *res = SUCCESS;
      return;
    } END_PIE
  }
}

