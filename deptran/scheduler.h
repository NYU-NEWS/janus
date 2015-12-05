#pragma once
#include "__dep__.h"
#include "constants.h"

namespace rococo {

class DTxn;
class RequestHeader;
class TxnRegistry;
class Executor;

class Scheduler {
 public:
  std::map<i64, DTxn *> dtxns_;
  std::map<i64, mdb::Txn *> mdb_txns_;
  map<cmdid_t, Executor*> executors_;

  mdb::TxnMgr *mdb_txn_mgr_;
  int mode_;
  Recorder *recorder_;
  TxnRegistry *txn_reg_;

  Scheduler();
  Scheduler(int mode);
  ~Scheduler();

  DTxn *get(i64 tid);
  DTxn *Create(i64 tid, bool ro = false);
  DTxn *GetOrCreate(i64 tid, bool ro = false);
  void Destroy(i64 tid);

  Executor* GetExecutor(txnid_t txn_id);
  Executor* CreateExecutor(txnid_t txn_id);
  Executor* GetOrCreateExecutor(txnid_t txn_id);
  void DestroyExecutor(txnid_t txn_id);

  inline int get_mode() { return mode_; }

  // Below are function calls that go deeper into the mdb.
  // They are merged from the called TxnRunner.

  inline mdb::Table
  *get_table(const string &name) {
    return mdb_txn_mgr_->get_table(name);
  }

  virtual mdb::Txn *get_mdb_txn(const i64 tid);
  virtual mdb::Txn *get_mdb_txn(const RequestHeader &req);
  virtual mdb::Txn *del_mdb_txn(const i64 tid);

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

};


} // namespace rococo
