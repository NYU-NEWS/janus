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

namespace rococo {

mdb::Txn *Scheduler::del_mdb_txn(const i64 tid) {
  mdb::Txn *txn = NULL;
  std::map<i64, mdb::Txn *>::iterator it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    verify(0);
  }
  else {
    txn = it->second;
  }
  mdb_txns_.erase(it);
  return txn;
}

mdb::Txn *Scheduler::get_mdb_txn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    //verify(IS_MODE_2PL);
    txn = mdb_txn_mgr_->start(tid);
    //XXX using occ lazy mode: increment version at commit time
    if (mode_ == MODE_OCC) {
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

mdb::Txn *Scheduler::get_mdb_txn(const RequestHeader &header) {
  mdb::Txn *txn = nullptr;
  if (mode_ == MODE_NONE
      || mode_ == MODE_RCC
      || mode_ == MODE_RO6) {
    if (mdb_txns_.empty()) {
      txn = mdb_txn_mgr_->start(0);
      mdb_txns_[0] = txn;
    } else {
      txn = mdb_txns_.begin()->second;
    }
  } else {
    txn = get_mdb_txn(header.tid);
  }
  verify(txn != nullptr);
  return txn;
}


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


Scheduler::Scheduler() {
  //  verify(DTxnMgr::txn_mgr_s == NULL);
//  DTxnMgr::txn_mgr_s = this;

  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    // TODO free this
    recorder_ = new Recorder(path);
  }
}

Scheduler::Scheduler(int mode) {
  Scheduler();
  mode_ = mode;
  switch (mode) {
    case MODE_NONE:
    case MODE_RPC_NULL:
      mdb_txn_mgr_ = new mdb::TxnMgrUnsafe();
      break;
    case MODE_OCC:
      mdb_txn_mgr_ = new mdb::TxnMgrOCC();
      break;
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
                            new mdb::SortedTable(schema));
  }
}

DTxn*Scheduler::Create(i64 tid, bool ro) {
  verify(dtxns_[tid] == nullptr);
  Log_debug("create tid %ld\n", tid);
  DTxn* dtxn = Frame().CreateDTxn(tid, ro, this);
  dtxns_[tid] = dtxn;
  dtxn->recorder_ = this->recorder_;
  verify(txn_reg_);
  dtxn->txn_reg_ = txn_reg_;
  return dtxn;
}

DTxn*Scheduler::GetOrCreate(i64 tid, bool ro) {
  auto it = dtxns_.find(tid);
  if (it == dtxns_.end()) {
    return Create(tid, ro);
  } else {
    return it->second;
  }
}

void Scheduler::Destroy(i64 tid) {
  Log_debug("destroy tid %ld\n", tid);
  auto it = dtxns_.find(tid);
  verify(it != dtxns_.end());
  delete it->second;
  dtxns_.erase(it);
}

DTxn*Scheduler::get(i64 tid) {
  //Log_debug("DTxnMgr::get(%ld)\n", tid);
  auto it = dtxns_.find(tid);
  verify(it != dtxns_.end());
  return it->second;
}

} // namespace rococo