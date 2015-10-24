#include "all.h"
#include "frame.h"

namespace rococo {

//void entry_t::touch(Vertex<TxnInfo> *tv, bool immediate ) {
//    int8_t edge_type = immediate ? EDGE_I : EDGE_D;
//    if (last_ != NULL) {
//        last_->to_[tv] |= edge_type;
//        tv->from_[last_] |= edge_type;
//    } else {
//        last_ = tv;
//    }
//}


mdb::Txn *DTxnMgr::del_mdb_txn(const i64 tid) {
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

mdb::Txn *DTxnMgr::get_mdb_txn(const i64 tid) {
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

mdb::Txn *DTxnMgr::get_mdb_txn(const RequestHeader &header) {
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
  }
  else {
    txn = get_mdb_txn(header.tid);
  }
  verify(txn != nullptr);
  return txn;
}


// TODO move this to the dtxn class
void DTxnMgr::get_prepare_log(i64 txn_id,
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

//DTxnMgr *DTxnMgr::txn_mgr_s = NULL;
map<uint32_t, DTxnMgr*> DTxnMgr::txn_mgrs_s;


DTxnMgr::DTxnMgr(int mode) {
  mode_ = mode;
  switch (mode) {
    case MODE_NONE:
    case MODE_RPC_NULL:
      mdb_txn_mgr_ = new mdb::TxnMgrUnsafe();
      break;
    case MODE_2PL:
      mdb_txn_mgr_ = new mdb::TxnMgr2PL();
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

//  verify(DTxnMgr::txn_mgr_s == NULL);
//  DTxnMgr::txn_mgr_s = this;

  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    // TODO free this
    recorder_ = new Recorder(path);
  }
}


DTxnMgr::~DTxnMgr() {
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

void DTxnMgr::reg_table(const std::string &name,
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

DTxn *DTxnMgr::Create(i64 tid, bool ro) {

  DTxn *ret = nullptr;

  verify(dtxns_[tid] == nullptr);
  dtxns_[tid] = Frame().CreateDTxn(tid, ro, this);
  ret->recorder_ = this->recorder_;
  return ret;
}


DTxn::~DTxn() {

}

mdb::ResultSet DTxn::query_in(Table *tbl,
                              const mdb::SortedMultiKey &low,
                              const mdb::SortedMultiKey &high,
                              mdb::symbol_t order) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query_in(tbl, low, high, order);
}

mdb::ResultSet DTxn::query_in(Table *tbl,
                              const mdb::MultiBlob &low,
                              const mdb::MultiBlob &high,
                              bool retrieve,
                              int64_t pid,
                              mdb::symbol_t order) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query_in(tbl, low, high, retrieve, pid, order);
}


mdb::ResultSet DTxn::query(Table *tbl, const mdb::Value &kv) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, kv);
}

mdb::ResultSet DTxn::query(Table *tbl, const Value &kv, bool retrieve, int64_t pid) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, kv, retrieve, pid);
}

mdb::ResultSet DTxn::query(Table *tbl, const mdb::MultiBlob &mb) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, mb);
}

mdb::ResultSet DTxn::query(
    mdb::Table *tbl,
    const mdb::MultiBlob &mb,
    bool retrieve,
    int64_t pid) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, mb, retrieve, pid);
}

bool DTxn::read_column(
    mdb::Row *row,
    mdb::column_id_t col_id,
    Value *value) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->read_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool DTxn::read_columns(
    Row *row,
    const std::vector<column_id_t> &col_ids,
    std::vector<Value> *values) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->read_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}

bool DTxn::write_column(Row *row, column_id_t col_id, const Value &value) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->write_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool DTxn::write_columns(
    Row *row,
    const std::vector<column_id_t> &col_ids,
    const std::vector<Value> &values) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->write_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}


bool DTxn::insert_row(Table *tbl, Row *row) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->insert_row(tbl, row);
}
bool DTxn::remove_row(Table *tbl, Row *row) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->remove_row(tbl, row);
}

mdb::Table *DTxn::get_table(const std::string &tbl_name) const {
  return mgr_->get_table(tbl_name);
}

} // namespace rococo
