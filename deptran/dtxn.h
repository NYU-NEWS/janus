#pragma once

#include "__dep__.h"
#include "rcc/dep_graph.h"
#include "rcc/rcc_row.h"
#include "ro6/ro6_row.h"
#include "multi_value.h"
#include "txn_reg.h"

namespace rococo {

using mdb::Row;
using mdb::Table;
using mdb::column_id_t;


#define IS_MODE_RCC (Config::GetConfig()->get_mode() == MODE_RCC)
#define IS_MODE_RO6 (Config::GetConfig()->get_mode() == MODE_RO6)
#define IS_MODE_2PL (Config::GetConfig()->get_mode() == MODE_2PL)
#define IS_MODE_OCC (Config::GetConfig()->get_mode() == MODE_OCC)
#define IS_MODE_NONE (Config::GetConfig()->get_mode() == MODE_NONE)

struct entry_t {
  Vertex<TxnInfo> *last_ = NULL; // last transaction(write) that touches this item. (arriving order)

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


//typedef std::unordered_map<
//    char *, 
//    std::unordered_map<
//        mdb::MultiBlob, 
//        mdb::Row *, 
//        mdb::MultiBlob::hash> > row_map_t;
//typedef std::unordered_map<cell_locator_t, entry_t *, cell_locator_t_hash> cell_entry_map_t;
// in charge of storing the pre-defined procedures
//



class Scheduler;

class DTxn {
 public:
  int64_t tid_;
  Scheduler *mgr_;
  int phase_;
  mdb::Txn *mdb_txn_;
  Recorder *recorder_ = NULL;
  TxnRegistry *txn_reg_;

  DTxn() = delete;

  DTxn(i64 tid, Scheduler *mgr)
      : tid_(tid), mgr_(mgr), phase_(0), mdb_txn_(nullptr) { }

  virtual mdb::Row *create(
      const mdb::Schema *schema,
      const std::vector<mdb::Value> &values) = 0;

  virtual bool read_column(
      mdb::Row *row,
      mdb::column_id_t col_id,
      Value *value);

  virtual bool read_columns(
      Row *row,
      const std::vector<column_id_t> &col_ids,
      std::vector<Value> *values
  );

  virtual bool write_column(
      Row *row,
      column_id_t col_id,
      const Value &value
  );

  virtual bool insert_row(Table *tbl, Row *row);

  virtual bool remove_row(Table *tbl, Row *row);

  virtual mdb::ResultSet query(Table *tbl, const mdb::Value &kv);

  virtual mdb::ResultSet query(
      Table *tbl,
      const mdb::Value &kv,
      bool retrieve,
      int64_t pid
  );

  virtual mdb::ResultSet query(Table *tbl, const mdb::MultiBlob &mb);

  virtual mdb::ResultSet query(
      mdb::Table *tbl,
      const mdb::MultiBlob &mb,
      bool retrieve,
      int64_t pid
  );

  virtual mdb::ResultSet query_in(
      Table *tbl,
      const mdb::SortedMultiKey &low,
      const mdb::SortedMultiKey &high,
      mdb::symbol_t order = mdb::symbol_t::ORD_ASC
  );

  virtual mdb::ResultSet query_in(
      Table *tbl,
      const mdb::MultiBlob &low,
      const mdb::MultiBlob &high,
      bool retrieve,
      int64_t pid,
      mdb::symbol_t order = mdb::symbol_t::ORD_ASC
  );

  mdb::Table *get_table(const std::string &tbl_name) const;

  bool write_columns(
      Row *row,
      const std::vector<column_id_t> &col_ids,
      const std::vector<Value> &values
  );

  virtual ~DTxn();
};

} // namespace rococo
