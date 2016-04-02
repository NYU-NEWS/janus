#pragma once
#include "__dep__.h"
#include "constants.h"
#include "command.h"

namespace rococo {

class DTxn;
class TxnRegistry;
class Executor;
class Coordinator;
class Frame;
class Scheduler {
 public:
  map<i64, DTxn *> dtxns_;
  map<i64, mdb::Txn *> mdb_txns_;
  map<cmdid_t, Executor*> executors_ = {};
  function<void(ContainerCommand&)> learner_action_ =
      [] (ContainerCommand&) -> void {verify(0);};

  mdb::TxnMgr *mdb_txn_mgr_;
  int mode_;
  Recorder *recorder_ = nullptr;
  Frame *frame_ = nullptr;
  Frame *rep_frame_ = nullptr;
  Scheduler* rep_sched_ = nullptr;
//  Coordinator* rep_coord_ = nullptr;
  TxnRegistry* txn_reg_ = nullptr;
  parid_t partition_id_;
  std::recursive_mutex mtx_;

  Scheduler();
  Scheduler(int mode);
  virtual ~Scheduler();

  virtual void SetPartitionId(parid_t par_id) {
    partition_id_ = par_id;
  }

  Coordinator*CreateRepCoord();
  DTxn *GetDTxn(i64 tid);
  DTxn *CreateDTxn(i64 tid, bool ro = false);
  DTxn *GetOrCreateDTxn(i64 tid, bool ro = false);
  void DestroyDTxn(i64 tid);

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

  virtual mdb::Txn* GetMTxn(const i64 tid);
  virtual mdb::Txn *GetOrCreateMTxn(const i64 tid);
//  virtual mdb::Txn *get_mdb_txn(const RequestHeader &req);
  virtual mdb::Txn *RemoveMTxn(const i64 tid);

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

  void RegLearnerAction(function<void(ContainerCommand&)> learner_action) {
    learner_action_ = learner_action;
  }

  virtual void OnLearn(ContainerCommand& cmd) {verify(0);};
};


} // namespace rococo
