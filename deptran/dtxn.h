/**
 * This class is currently mixed with three kinds of information:
 *   1. concurrency control metadata.
 *   2. database transaction logic and workspace
 *   3. temporary workspace for database transactions (version, temp values..)
 *   4. database interface
 * Shall we split the three into three classes? Or is this fine?
 */

#pragma once

#include "__dep__.h"
#include "rcc/rcc_row.h"
#include "ro6/ro6_row.h"
#include "multi_value.h"
#include "txn_reg.h"
#include "deptran/scheduler.h"
namespace rococo {

using mdb::Row;
using mdb::Table;
using mdb::column_id_t;

#define TXN_BYPASS   (0x01)
#define TXN_SAFE     (0x02)
#define TXN_INSTANT  (0x02)
#define TXN_DEFERRED (0x04)


#define IS_MODE_RCC (Config::GetConfig()->get_mode() == MODE_RCC)
#define IS_MODE_RO6 (Config::GetConfig()->get_mode() == MODE_RO6)
#define IS_MODE_2PL (Config::GetConfig()->get_mode() == MODE_2PL)
#define IS_MODE_OCC (Config::GetConfig()->get_mode() == MODE_OCC)
#define IS_MODE_NONE (Config::GetConfig()->get_mode() == MODE_NONE)

struct entry_t {
  void* last_{nullptr}; // last transaction(write) that touches this
  // item. (arriving order)

  const entry_t &operator=(const entry_t &rhs) {
    last_ = rhs.last_;
    return *this;
  }

  entry_t() {
  }

  entry_t(const entry_t &o) {
    last_ = o.last_;
  }
};

class Scheduler;

class DTxn {
 public:
  txnid_t tid_;
  epoch_t epoch_{0};
  Scheduler *sched_;
  int phase_;
  mdb::Txn *mdb_txn_ = nullptr;
  Recorder *recorder_ = NULL;
  TxnRegistry *txn_reg_{nullptr};

  bool committed_{false};
  bool aborted_{false};

  map<int64_t, mdb::Row*> context_row_;
  map<int64_t, Value> context_value_;
  map<int64_t, mdb::ResultSet> context_rs_;

  DTxn() = delete;

  DTxn(epoch_t epoch, txnid_t tid, Scheduler *mgr)
      : epoch_(epoch),
        tid_(tid),
        sched_(mgr),
        phase_(0),
        context_row_(),
        context_value_(),
        context_rs_(),
        mdb_txn_(nullptr) {}

  mdb::Txn* mdb_txn();

  virtual mdb::Row *CreateRow(const mdb::Schema *schema,
                              const std::vector<mdb::Value> &values);

  virtual bool ReadColumn(mdb::Row *row,
                          mdb::column_id_t col_id,
                          Value *value,
                          int hint_flag = TXN_INSTANT);

  virtual bool ReadColumns(Row *row,
                           const std::vector<column_id_t> &col_ids,
                           std::vector<Value> *values,
                           int hint_flag = TXN_INSTANT);

  virtual bool WriteColumn(Row *row,
                           column_id_t col_id,
                           const Value &value,
                           int hint_flag = TXN_INSTANT);

  virtual bool WriteColumns(Row *row,
                            const std::vector<column_id_t> &col_ids,
                            const std::vector<Value> &values,
                            int hint_flag = TXN_INSTANT);

  virtual bool InsertRow(Table *tbl, Row *row);

  virtual mdb::Row* Query(mdb::Table *tbl,
                          const mdb::MultiBlob &mb,
                          int64_t row_context_id = 0);

  virtual mdb::ResultSet QueryIn(Table *tbl,
                                 const mdb::MultiBlob &low,
                                 const mdb::MultiBlob &high,
                                 mdb::symbol_t order = mdb::symbol_t::ORD_ASC,
                                 int rs_context_id = 0);

  virtual mdb::Table *GetTable(const std::string &tbl_name) const;

  virtual ~DTxn();
};

} // namespace rococo
