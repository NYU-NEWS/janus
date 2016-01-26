//
// Created by lamont on 1/11/16.
//
#pragma once
#include "deptran/dtxn.h"
#include "deptran/scheduler.h"
using namespace mdb;
using namespace rococo;

namespace mdcc {
  using rococo::DTxn;
  using rococo::Scheduler;

  class MdccDTxn: public DTxn {
  protected:
  public:
    MdccDTxn() = delete;
    MdccDTxn(txnid_t tid, Scheduler* mgr) : DTxn(tid, mgr) {}

    virtual bool ReadColumn(Row *row, column_id_t col_id, Value *value,
                            int hint_flag) override;
    virtual bool ReadColumns(Row *row, const vector<column_id_t> &col_ids,
                             std::vector<Value> *values, int hint_flag) override;
    virtual bool WriteColumn(Row *row, column_id_t col_id, const Value &value,
                             int hint_flag) override;
    virtual bool WriteColumns (Row *row, const vector<column_id_t> &col_ids,
                               const std::vector<Value> &values, int hint_flag) override;
    virtual bool InsertRow(Table *tbl, Row *row) override;
    virtual Row *Query(Table *tbl, const MultiBlob &mb, int64_t row_context_id) override;
    virtual ResultSet QueryIn (Table *tbl, const MultiBlob &low,
                               const MultiBlob &high, symbol_t order,
                               int rs_context_id) override;
    virtual Table *GetTable(const std::string &tbl_name) const override;
    virtual Row* create(const Schema *schema, const std::vector<Value> &values) {
      verify(0);
    }
  };
}
