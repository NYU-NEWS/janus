#pragma once

#include "__dep__.h"
#include "dep_graph.h"
#include "rcc_row.h"
#include "ro6_row.h"
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

#define IN_PHASE_1 (dtxn->phase_ == 1)
#define TPL_PHASE_1 (output_size == nullptr)
#define RO6_RO_PHASE_1 ((Config::GetConfig()->get_mode() == MODE_RO6) && ((RO6DTxn*)dtxn)->read_only_ && dtxn->phase_ == 1)

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



class DTxnMgr;

class DTxn {
 public:
  int64_t tid_;
  DTxnMgr *mgr_;
  int phase_;
  mdb::Txn *mdb_txn_;
  Recorder *recorder_ = NULL;
  TxnRegistry *txn_reg_;

  DTxn() = delete;

  DTxn(i64 tid, DTxnMgr *mgr)
      : tid_(tid), mgr_(mgr), phase_(0), mdb_txn_(nullptr) { }

  virtual mdb::Row *create(
      const mdb::Schema *schema,
      const std::vector<mdb::Value> &values
  ) = 0;

  virtual bool read_column(
      mdb::Row *row,
      mdb::column_id_t col_id,
      Value *value
  );

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

#include "rcc.h"
#include "ro6.h"
#include "tpl.h"

class DTxnMgr {
 public:
  std::map<i64, DTxn *> dtxns_;
  std::map<i64, mdb::Txn *> mdb_txns_;
  mdb::TxnMgr *mdb_txn_mgr_;
  int mode_;
  Recorder *recorder_;

  DTxnMgr(int mode);

  ~DTxnMgr();

  DTxn *Create(i64 tid, bool ro = false);

  void destroy(i64 tid) {
    auto it = dtxns_.find(tid);
    verify(it != dtxns_.end());
    delete it->second;
    dtxns_.erase(it);
  }

  DTxn *get(i64 tid) {
    auto it = dtxns_.find(tid);
    verify(it != dtxns_.end());
    return it->second;
  }

  DTxn *GetOrCreate(i64 tid, bool ro = false) {
    auto it = dtxns_.find(tid);
    if (it == dtxns_.end()) {
      return Create(tid, ro);
    } else {
      return it->second;
    }
  }

  inline int get_mode() { return mode_; }


  // Below are function calls that go deeper into the mdb.
  // They are merged from the called TxnRunner.

  inline mdb::Table
  *get_table(const string &name) {
    return mdb_txn_mgr_->get_table(name);
  }

  mdb::Txn *get_mdb_txn(const i64 tid);

  mdb::Txn *get_mdb_txn(const RequestHeader &req);

  mdb::Txn *del_mdb_txn(const i64 tid);

  void get_prepare_log(i64 txn_id,
                       const std::vector<i32> &sids,
                       std::string *str
  );


  // TODO: (Shuai: I am not sure this is supposed to be here.)
  // I think it used to initialized the database?
  // So it should be somewhere else?
  void reg_table(const string &name,
                 mdb::Table *tbl
  );

//  static DTxnMgr *txn_mgr_s;
  static map<uint32_t, DTxnMgr*> txn_mgrs_s;

  static DTxnMgr* CreateTxnMgr(uint32_t site_id) {
    auto mgr = new DTxnMgr(Config::config_s->mode_);
    auto it = txn_mgrs_s.find(site_id);
    verify(it == txn_mgrs_s.end());
    txn_mgrs_s[site_id] = mgr;
    return mgr;
  }

  static DTxnMgr* GetTxnMgr(uint32_t site_id) {
    auto it = txn_mgrs_s.find(site_id);
    verify(it != txn_mgrs_s.end());
    return it->second;
  }

//  static DTxnMgr *get_sole_mgr() {
//    verify(txn_mgr_s != NULL);
//    return txn_mgr_s;
//  }
};

} // namespace rococo
