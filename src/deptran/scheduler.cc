#include "__dep__.h"
#include "constants.h"
#include "tx.h"
#include "scheduler.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "marshal-value.h"
#include "procedure.h"
#include "rcc_rpc.h"
#include "frame.h"
#include "bench/tpcc/workload.h"
#include "executor.h"
#include "coordinator.h"

namespace janus {

shared_ptr<Tx> TxLogServer::CreateTx(epoch_t epoch, txnid_t tid, bool
read_only) {
  Log_debug("create tid %ld", tid);
  verify(dtxns_.find(tid) == dtxns_.end());
  if (epoch == 0) {
    epoch = epoch_mgr_.curr_epoch_;
  }
  verify(epoch_mgr_.IsActive(epoch));
  auto dtxn = frame_->CreateTx(epoch, tid, read_only, this);
  if (dtxn != nullptr) {
    dtxns_[tid] = dtxn;
    dtxn->recorder_ = this->recorder_;
    dtxn->txn_reg_ = txn_reg_;
    verify(txn_reg_ != nullptr);
    verify(dtxn->tid_ == tid);
  } else {
    verify(0);
  }
  if (epoch_enabled_) {
    epoch_mgr_.AddToEpoch(epoch, tid);
    TriggerUpgradeEpoch();
  }
  dtxn->sched_ = this;
  return dtxn;
}

shared_ptr<Tx> TxLogServer::CreateTx(txnid_t tx_id, bool ro) {
  Log_debug("create tid %" PRIx64, tx_id);
  verify(dtxns_.find(tx_id) == dtxns_.end());
  auto dtxn = frame_->CreateTx(epoch_mgr_.curr_epoch_, tx_id, ro, this);
  if (dtxn != nullptr) {
    dtxns_[tx_id] = dtxn;
    dtxn->recorder_ = this->recorder_;
    verify(txn_reg_);
    dtxn->txn_reg_ = txn_reg_;
    verify(dtxn->tid_ == tx_id);
    if (epoch_enabled_) {
      epoch_mgr_.AddToCurrent(tx_id);
      TriggerUpgradeEpoch();
    }
    dtxn->sched_ = this;
  } else {
    // for multi-paxos this would happen.
    // verify(0);
  }
  return dtxn;
}

shared_ptr<Tx> TxLogServer::GetOrCreateTx(txnid_t tid, bool ro) {
  shared_ptr<Tx> ret = nullptr;
  auto it = dtxns_.find(tid);
  if (it == dtxns_.end()) {
    ret = CreateTx(tid, ro);
  } else {
    ret = it->second;
  }
  verify(ret != nullptr);
  verify(ret->tid_ == tid);
  return ret;
}

void TxLogServer::DestroyTx(i64 tid) {
  Log_debug("destroy tid %ld", tid);
  auto it = dtxns_.find(tid);
  verify(it != dtxns_.end());
  dtxns_.erase(it);
}

shared_ptr<Tx> TxLogServer::GetTx(txnid_t tid) {
  // Log_debug("DTxnMgr::get(%ld)\n", tid);
  auto it = dtxns_.find(tid);
  // verify(it != dtxns_.end());
  if (it != dtxns_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

mdb::Txn *TxLogServer::GetMTxn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    verify(0);
  } else {
    txn = it->second;
  }
  return txn;
}

mdb::Txn *TxLogServer::RemoveMTxn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  verify(it != mdb_txns_.end());
  txn = it->second;
  mdb_txns_.erase(it);
  return txn;
}

mdb::Txn *TxLogServer::GetOrCreateMTxn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    txn = mdb_txn_mgr_->start(tid);
    // using occ lazy mode: increment version at commit time
    auto mode = Config::GetConfig()->tx_proto_;
    if (mode == MODE_OCC || mode == MODE_MDCC) {
      ((mdb::TxnOCC *) txn)->set_policy(mdb::OCC_LAZY);
    }
    auto ret = mdb_txns_.insert(std::pair<i64, mdb::Txn *>(tid, txn));
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

// TODO move this to the dtxn class
void TxLogServer::get_prepare_log(i64 txn_id,
                                  const std::vector<i32> &sids,
                                  std::string *str) {
  auto it = mdb_txns_.find(txn_id);
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
  memcpy((void *) (str->data() + len),
         (void *) &prepare_tag,
         sizeof(prepare_tag));
  len += sizeof(prepare_tag);
  verify(len == str->size());

  // marshal related servers
  uint32_t num_servers = sids.size();
  str->resize(len + sizeof(num_servers) + sizeof(i32) * num_servers);
  memcpy((void *) (str->data() + len),
         (void *) &num_servers,
         sizeof(num_servers));
  len += sizeof(num_servers);
  for (uint32_t i = 0; i < num_servers; i++) {
    memcpy((void *) (str->data() + len), (void *) (&(sids[i])), sizeof(i32));
    len += sizeof(i32);
  }
  verify(len == str->size());

  switch (mode_) {
    case MODE_2PL:
    case MODE_OCC:((mdb::Txn2PL *) it->second)->marshal_stage(*str);
      break;
    default:verify(0);
  }
}

TxLogServer::TxLogServer() : mtx_() {
  mdb_txn_mgr_ = make_shared<mdb::TxnMgrUnsafe>();
  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    // TODO free this
//    recorder_ = new Recorder(path);
  }
}

Coordinator *TxLogServer::CreateRepCoord() {
  Coordinator *coord;
  static cooid_t cid = 0;
  int32_t benchmark = 0;
  static id_t id = 0;
  verify(rep_frame_ != nullptr);
  coord = rep_frame_->CreateCoordinator(cid++,
                                        Config::GetConfig(),
                                        benchmark,
                                        nullptr,
                                        id++,
                                        txn_reg_);
  coord->par_id_ = partition_id_;
  coord->loc_id_ = this->loc_id_;
  return coord;
}

TxLogServer::TxLogServer(int mode) : TxLogServer() {
  mode_ = mode;
  switch (mode) {
    case MODE_MDCC:
    case MODE_OCC:
      mdb_txn_mgr_ = make_shared<mdb::TxnMgrOCC>();
      break;
    case MODE_NONE:
    case MODE_RPC_NULL:
    case MODE_RCC:
    case MODE_RO6:
      mdb_txn_mgr_ = make_shared<mdb::TxnMgrUnsafe>();
      break;
    default:verify(0);
  }
}

TxLogServer::~TxLogServer() {
  auto it = mdb_txns_.begin();
  for (; it != mdb_txns_.end(); it++)
    Log::info("tid: %ld still running", it->first);
  if (it != mdb_txns_.end() && it->second) {
    delete it->second;
    it->second = NULL;
  }
  mdb_txns_.clear();
}

/**
 *
 * @param txn_box
 * @param inn_id, if 0, execute all pieces.
 */
void TxLogServer::Execute(Tx &txn_box,
                          innid_t inn_id) {
  if (inn_id == 0) {
    for (auto &pair : txn_box.paused_pieces_) {
      auto &up_pause = pair.second;
      verify(up_pause);
      up_pause->Set(1);
    }
    txn_box.paused_pieces_.clear();
  } else {
    auto &up_pause = txn_box.paused_pieces_[inn_id];
    verify(up_pause);
    up_pause->Set(1);
    txn_box.paused_pieces_.erase(inn_id);
  }
}

void TxLogServer::reg_table(const std::string &name,
                            mdb::Table *tbl) {
  verify(mdb_txn_mgr_ != NULL);
  mdb_txn_mgr_->reg_table(name, tbl);
  if (name == TPCC_TB_ORDER) {
    mdb::Schema *schema = new mdb::Schema();
    const mdb::Schema *o_schema = tbl->schema();
    mdb::Schema::iterator it = o_schema->begin();
    for (; it != o_schema->end(); it++)
      if (it->indexed)
        if (it->name != "o_id")
          schema->add_column(it->name.c_str(), it->type, true);
    schema->add_column("o_c_id", Value::I32, true);
    schema->add_column("o_id", Value::I32, false);
    mdb_txn_mgr_->reg_table(TPCC_TB_ORDER_C_ID_SECONDARY,
                            new mdb::SortedTable(name, schema));
  }
}

void TxLogServer::DestroyExecutor(txnid_t txn_id) {
  Log_debug("destroy tid %ld\n", txn_id);
  auto it = executors_.find(txn_id);
  verify(it != executors_.end());
  auto exec = it->second;
  executors_.erase(it);
  delete exec;
}

void TxLogServer::TriggerUpgradeEpoch() {
  if (site_id_ == 0) {
    auto t_now = std::time(nullptr);
    auto d = std::difftime(t_now, last_upgrade_time_);
    if (d < EPOCH_DURATION || in_upgrade_epoch_) {
      return;
    }
    last_upgrade_time_ = t_now;
    in_upgrade_epoch_ = true;
    epoch_t epoch = epoch_mgr_.curr_epoch_;
    commo()->SendUpgradeEpoch(epoch,
                              std::bind(&TxLogServer::UpgradeEpochAck,
                                        this,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        std::placeholders::_3));
  }
}

void TxLogServer::UpgradeEpochAck(parid_t par_id,
                                  siteid_t site_id,
                                  int32_t res) {
  auto parids = Config::GetConfig()->GetAllPartitionIds();
  epoch_replies_[par_id][site_id] = res;
  if (epoch_replies_.size() < parids.size()) {
    return;
  }
  for (auto &pair: epoch_replies_) {
    auto par_id = pair.first;
    auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
    verify(epoch_replies_[par_id].size() <= par_size);
    if (epoch_replies_[par_id].size() != par_size) {
      return;
    }
  }

  epoch_t smallest_inactive = 0xFFFFFFFF;
  for (auto &pair1 : epoch_replies_) {
    for (auto &pair2 : pair1.second) {
      if (smallest_inactive > pair2.second) {
        smallest_inactive = pair2.second;
      }
    }
  }
  in_upgrade_epoch_ = false;
  epoch_replies_.clear();
  int x = 5;
  if (smallest_inactive >= x) {
    epoch_t epoch_to_truncate = smallest_inactive - x;
    if (epoch_to_truncate >= epoch_mgr_.oldest_active_) {
      Log_info("truncate epoch %d", epoch_to_truncate);
      commo()->SendTruncateEpoch(epoch_to_truncate);
    }
  }
}

int32_t TxLogServer::OnUpgradeEpoch(uint32_t old_epoch) {
  epoch_mgr_.GrowActive();
  epoch_mgr_.GrowBuffer();
  return epoch_mgr_.CheckBufferInactive();
}

} // namespace janus
