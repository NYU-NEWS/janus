#include "__dep__.h"
#include "constants.h"
#include "dtxn.h"
#include "scheduler.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "marshal-value.h"
#include "rcc_rpc.h"
#include "frame.h"
#include "bench/tpcc/piece.h"
#include "executor.h"
#include "coordinator.h"

namespace rococo {

DTxn* Scheduler::CreateDTxn(i64 tid, bool ro) {
  Log_debug("create tid %ld", tid);
  verify(dtxns_.find(tid) == dtxns_.end());
  DTxn* dtxn = frame_->CreateDTxn(tid, ro, this);
  if (dtxn != nullptr) {
    dtxns_[tid] = dtxn;
    dtxn->recorder_ = this->recorder_;
    verify(txn_reg_);
    dtxn->txn_reg_ = txn_reg_;
  }
  return dtxn;
}

DTxn* Scheduler::GetOrCreateDTxn(i64 tid, bool ro) {
  auto it = dtxns_.find(tid);
  if (it == dtxns_.end()) {
    return CreateDTxn(tid, ro);
  } else {
    return it->second;
  }
}

void Scheduler::DestroyDTxn(i64 tid) {
  Log_debug("destroy tid %ld\n", tid);
  auto it = dtxns_.find(tid);
  verify(it != dtxns_.end());
  delete it->second;
  dtxns_.erase(it);
}

DTxn* Scheduler::GetDTxn(i64 tid) {
  //Log_debug("DTxnMgr::get(%ld)\n", tid);
  auto it = dtxns_.find(tid);
//  verify(it != dtxns_.end());
  if (it != dtxns_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

mdb::Txn* Scheduler::GetMTxn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    verify(0);
  } else {
    txn = it->second;
  }
  return txn;
}

mdb::Txn* Scheduler::RemoveMTxn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  std::map<i64, mdb::Txn *>::iterator it = mdb_txns_.find(tid);
  verify(it != mdb_txns_.end());
  txn = it->second;
  mdb_txns_.erase(it);
  return txn;
}

mdb::Txn* Scheduler::GetOrCreateMTxn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    //verify(IS_MODE_2PL);
    txn = mdb_txn_mgr_->start(tid);
    //XXX using occ lazy mode: increment version at commit time
    auto mode = Config::GetConfig()->cc_mode_;
    if (mode == MODE_OCC || mode == MODE_MDCC) {
      ((mdb::TxnOCC *) txn)->set_policy(mdb::OCC_LAZY);
    }
    std::pair<std::map<i64, mdb::Txn *>::iterator, bool> ret
        = mdb_txns_.insert(std::pair<i64, mdb::Txn *>(tid, txn));
    verify(ret.second);
  } else {
    txn = it->second;
  }

  if (IS_MODE_2PL) {
    verify(mdb_txn_mgr_->rtti() == mdb::symbol_t::TXN_2PL);
    verify(txn->rtti() == mdb::symbol_t::TXN_2PL);
  } else {

  }

  verify(txn != nullptr);
  return txn;
}

//mdb::Txn* Scheduler::get_mdb_txn(const RequestHeader &header) {
//  mdb::Txn *txn = nullptr;
//  if (mode_ == MODE_NONE
//      || mode_ == MODE_RCC
//      || mode_ == MODE_RO6) {
//    if (mdb_txns_.empty()) {
//      txn = mdb_txn_mgr_->start(0);
//      mdb_txns_[0] = txn;
//    } else {
//      txn = mdb_txns_.begin()->second;
//    }
//  } else {
//    txn = GetOrCreateMTxn(header.tid);
//  }
//  verify(txn != nullptr);
//  return txn;
//}

// TODO move this to the dtxn class
void Scheduler::get_prepare_log(i64 txn_id,
                                const std::vector<i32> &sids,
                                std::string *str) {
  map<i64, mdb::Txn *>::iterator it = mdb_txns_.find(txn_id);
  verify(it != mdb_txns_.end() && it->second != NULL);

  // marshal txn_id
  uint64_t len = str->size();
  str->resize(len + sizeof(txn_id));
  memcpy((void *) (str->data()), (void *) (&txn_id), sizeof(txn_id));
  len += sizeof(txn_id);
  verify(len == str->size());

  // p denotes prepare log
  const char prepare_tag = 'p';
  str->resize(len + sizeof(prepare_tag));
  memcpy((void *) (str->data() + len), (void *) &prepare_tag, sizeof(prepare_tag));
  len += sizeof(prepare_tag);
  verify(len == str->size());

  // marshal related servers
  uint32_t num_servers = sids.size();
  str->resize(len + sizeof(num_servers) + sizeof(i32) * num_servers);
  memcpy((void *) (str->data() + len), (void *) &num_servers, sizeof(num_servers));
  len += sizeof(num_servers);
  for (uint32_t i = 0; i < num_servers; i++) {
    memcpy((void *) (str->data() + len), (void *) (&(sids[i])), sizeof(i32));
    len += sizeof(i32);
  }
  verify(len == str->size());

  switch (mode_) {
    case MODE_2PL:
    case MODE_OCC:
      ((mdb::Txn2PL *) it->second)->marshal_stage(*str);
      break;
    default:
      verify(0);
  }
}

Scheduler::Scheduler() : mtx_() {
  //  verify(DTxnMgr::txn_mgr_s == NULL);
//  DTxnMgr::txn_mgr_s = this;
  mdb_txn_mgr_ = new mdb::TxnMgrUnsafe();
  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    // TODO free this
    recorder_ = new Recorder(path);
  }
}

Coordinator* Scheduler::CreateRepCoord() {
//  if (rep_coord_) return rep_coord_;
  Coordinator *coord;
  // TODO
  cooid_t cid = 0;
  int32_t benchmark = 0;
  id_t id;
  verify(rep_frame_ != nullptr);
  coord = rep_frame_->CreateCoord(cid,
                                  Config::GetConfig(),
                                  benchmark,
                                  nullptr,
                                  id,
                                  txn_reg_);
  coord->par_id_ = partition_id_;
  return coord;
}

Scheduler::Scheduler(int mode) : Scheduler() {
  mode_ = mode;
  switch (mode) {
    case MODE_MDCC:
    case MODE_OCC:
      mdb_txn_mgr_ = new mdb::TxnMgrOCC();
      break;
    case MODE_NONE:
    case MODE_RPC_NULL:
    case MODE_RCC:
    case MODE_RO6:
      mdb_txn_mgr_ = new mdb::TxnMgrUnsafe(); //XXX is it OK to use unsafe for deptran
      break;
    default:
      verify(0);
  }
}

Scheduler::~Scheduler() {
  map<i64, mdb::Txn *>::iterator it = mdb_txns_.begin();
  for (; it != mdb_txns_.end(); it++)
    Log::info("tid: %ld still running", it->first);
  if (it->second) {
    delete it->second;
    it->second = NULL;
  }
  mdb_txns_.clear();
  if (mdb_txn_mgr_)
    delete mdb_txn_mgr_;
  mdb_txn_mgr_ = NULL;
}

void Scheduler::reg_table(const std::string &name,
                          mdb::Table *tbl) {
  verify(mdb_txn_mgr_ != NULL);
  mdb_txn_mgr_->reg_table(name, tbl);
  if (name == TPCC_TB_ORDER) {
    mdb::Schema *schema = new mdb::Schema();
    const mdb::Schema *o_schema = tbl->schema();
    mdb::Schema::iterator it = o_schema->begin();
    for (; it != o_schema->end(); it++)
      if (it->indexed) if (it->name != "o_id")
        schema->add_column(it->name.c_str(), it->type, true);
    schema->add_column("o_c_id", Value::I32, true);
    schema->add_column("o_id", Value::I32, false);
    mdb_txn_mgr_->reg_table(TPCC_TB_ORDER_C_ID_SECONDARY,
                            new mdb::SortedTable(name, schema));
  }
}

Executor* Scheduler::CreateExecutor(cmdid_t txn_id) {
  verify(executors_.find(txn_id) == executors_.end());
  Executor *exec = frame_->CreateExecutor(txn_id, this);
  verify(exec->sched_);
  DTxn* dtxn = CreateDTxn(txn_id);
  exec->dtxn_ = dtxn;
  executors_[txn_id] = exec;
  exec->recorder_ = this->recorder_;
  exec->txn_reg_ = txn_reg_;
  verify(txn_reg_);
  return exec;
}

Executor* Scheduler::GetOrCreateExecutor(cmdid_t txn_id) {
  Executor* exec = nullptr;
  auto it = executors_.find(txn_id);
  if (it == executors_.end()) {
    exec = CreateExecutor(txn_id);
  } else {
    exec = it->second;
    verify(exec->sched_ != nullptr);
  }
  return exec;
}

void Scheduler::DestroyExecutor(cmdid_t cmd_id) {
  Log_debug("destroy tid %ld\n", cmd_id);
  auto it = executors_.find(cmd_id);
  verify(it != executors_.end());
  auto exec = it->second;
  executors_.erase(it);
  delete exec;
}

Executor* Scheduler::GetExecutor(cmdid_t cmd_id) {
  //Log_debug("DTxnMgr::get(%ld)\n", tid);
  auto it = executors_.find(cmd_id);
  verify(it != executors_.end());
  return it->second;
}

} // namespace rococo
