#include "all.h"

namespace rococo {

void entry_t::touch(Vertex<TxnInfo> *tv, bool immediate ) {
    int8_t edge_type = immediate ? EDGE_I : EDGE_D;
    if (last_ != NULL) {
        last_->to_[tv] |= edge_type;
        tv->from_[last_] |= edge_type;
    } else {
        last_ = tv;
    }
}

int MultiValue::compare(const MultiValue& mv) const {
    int i = 0;
    for (i = 0; i < n_ && i < mv.n_; i++) {
        int r = v_[i].compare(mv.v_[i]);
        if (r != 0) {
            return r;
        }
    }
    if (i < n_) {
        return 1;
    } else if (i < mv.n_) {
        return -1;
    }
    return 0;
}

map<std::pair<base::i32, base::i32>,
    txn_handler_defer_pair_t> TxnRegistry::all_;
//map<std::pair<base::i32, base::i32>, TxnRegistry::LockSetOracle> TxnRegistry::lck_oracle_;


void DTxnMgr::pre_execute_2pl(const RequestHeader& header,
                           const std::vector<mdb::Value>& input,
                           rrr::i32* res,
                           std::vector<mdb::Value>* output,
                           DragonBall *db) {

    mdb::Txn2PL *txn = (mdb::Txn2PL *)get_mdb_txn(header);
    if (txn->is_wound()) {
        output->resize(0);
        *res = REJECT;
        db->trigger();
        return;
    }
    txn->init_piece(header.tid, header.pid, db, output);

    Log::debug("start reg lock");
    TxnRegistry::get(header).txn_handler(nullptr, header, input.data(), input.size(),
            res, NULL/*output*/, NULL/*output_size*/,
            NULL);
}

void DTxnMgr::pre_execute_2pl(const RequestHeader& header,
                           const Value *input,
                           rrr::i32 input_size,
                           rrr::i32* res,
                           mdb::Value* output,
                           rrr::i32* output_size,
                           DragonBall *db) {

    mdb::Txn2PL *txn = (mdb::Txn2PL *)get_mdb_txn(header);
    txn->init_piece(header.tid, header.pid, db, output, output_size);
    if (txn->is_wound()) {
        *output_size = 0;
        *res = REJECT;
        db->trigger();
        return;
    }
    TxnRegistry::get(header).txn_handler(nullptr, header, input, input_size,
            res, NULL/*output*/, NULL/*output_size*/,
            NULL);
}



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
    mdb::Txn *txn = NULL;
    std::map<i64, mdb::Txn *>::iterator it = mdb_txns_.find(tid);
    if (it == mdb_txns_.end()) {
        txn = mdb_txn_mgr_->start(tid);
        //XXX using occ lazy mode: increment version at commit time
        if (mode_ == MODE_OCC) {
            ((mdb::TxnOCC *)txn)->set_policy(mdb::OCC_LAZY);
        }
        std::pair<std::map<i64, mdb::Txn *>::iterator, bool> ret
                = mdb_txns_.insert(std::pair<i64, mdb::Txn *>(tid, txn));
        verify(ret.second);
    }
    else {
        txn = it->second;
    }
    return txn;
}

mdb::Txn *DTxnMgr::get_mdb_txn(const RequestHeader &header) {
    if (mode_ == MODE_NONE
            || mode_ == MODE_RCC) {
        if (mdb_txns_.empty()) {
            mdb::Txn *txn = mdb_txn_mgr_->start(0);
            mdb_txns_[0] = txn;
            return txn;
        }
        return mdb_txns_.begin()->second;
    }
    else {
        mdb::Txn *txn = NULL;
        txn = get_mdb_txn(header.tid);
        return txn;
    }
}


void DTxnMgr::get_prepare_log(i64 txn_id,
        const std::vector<i32> &sids,
        std::string *str) {
    map<i64, mdb::Txn *>::iterator it = mdb_txns_.find(txn_id);
    verify(it != mdb_txns_.end() && it->second != NULL);

    // marshal txn_id
    uint64_t len = str->size();
    str->resize(len + sizeof(txn_id));
    memcpy((void *)(str->data()), (void *)(&txn_id), sizeof(txn_id));
    len += sizeof(txn_id);
    verify(len == str->size());

    // p denotes prepare log
    const char prepare_tag = 'p';
    str->resize(len + sizeof(prepare_tag));
    memcpy((void *)(str->data() + len), (void *)&prepare_tag, sizeof(prepare_tag));
    len += sizeof(prepare_tag);
    verify(len == str->size());

    // marshal related servers
    uint32_t num_servers = sids.size();
    str->resize(len + sizeof(num_servers) + sizeof(i32) * num_servers);
    memcpy((void *)(str->data() + len), (void *)&num_servers, sizeof(num_servers));
    len += sizeof(num_servers);
    for (uint32_t i = 0; i < num_servers; i++) {
        memcpy((void *)(str->data() + len), (void *)(&(sids[i])), sizeof(i32));
        len += sizeof(i32);
    }
    verify(len == str->size());

    switch (mode_) {
        case MODE_2PL:
        case MODE_OCC:
            ((mdb::Txn2PL *)it->second)->marshal_stage(*str);
            break;
        default:
            verify(0);
    }
}

DTxnMgr* DTxnMgr::txn_mgr_s = NULL;

DTxnMgr::DTxnMgr(int mode) {
    mode_ = mode;
    switch(mode) {
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
        case MODE_DEPTRAN:
            mdb_txn_mgr_ = new mdb::TxnMgrUnsafe(); //XXX is it OK to use unsafe for deptran
            break;
        default:
            verify(0);
    }

    verify(DTxnMgr::txn_mgr_s == NULL);
    DTxnMgr::txn_mgr_s = this;
}


DTxnMgr::~DTxnMgr() {
    map<i64, mdb::Txn *>::iterator it = mdb_txns_.begin();
    for (; it != mdb_txns_.end(); it++)
        Log::info("tid: %ld still running",  it->first);
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
            if (it->indexed)
            if (it->name != "o_id")
                schema->add_column(it->name.c_str(), it->type, true);
        schema->add_column("o_c_id", Value::I32, true);
        schema->add_column("o_id", Value::I32, false);
        mdb_txn_mgr_->reg_table(TPCC_TB_ORDER_C_ID_SECONDARY,
                new mdb::SortedTable(schema));
    }
}

//void txn_entry_t::abort_2pl(
//        i64 tid,
//        std::map<i64, txn_entry_t *> *txn_map_s,
//        //pthread_mutex_t *txn_map_lock,
//        rrr::DeferredReply* defer
//        ) {
//    DragonBall *abort_db = new ConcurrentDragonBall(piece_map_.size(),
//            [this, tid, txn_map_s, /*txn_map_lock, */defer]() {
//            txn->abort();
//            //pthread_mutex_lock(&piece_map_mutex_);
//            for (unordered_map<i64, PieceStatus *>::iterator it
//                = piece_map_.begin(); it != piece_map_.end(); it++) {
//                delete it->second;
//            }
//            //pthread_mutex_unlock(&piece_map_mutex_);
//            delete this;
//            //pthread_mutex_lock(txn_map_lock);
//            txn_map_s->erase(tid);
//            //pthread_mutex_unlock(txn_map_lock);
//            defer->reply();
//            Log::debug("end reply abort");
//            });
//    //pthread_mutex_lock(&piece_map_mutex_);
//    std::unordered_map<i64, PieceStatus *> piece_map = piece_map_;
//    //pthread_mutex_unlock(&piece_map_mutex_);
//    for (unordered_map<i64, PieceStatus *>::iterator it = piece_map.begin();
//            it != piece_map.end(); it++) {
//        it->second->set_abort_dragonball(abort_db);
//    }
//    Log::debug("abort_2pl finish");
//}
//


//PieceStatus *TxnRunner::get_piece_status(
//        const RequestHeader &header,
//        bool insert) {
//    verify(running_mode_s == MODE_2PL);
//    //pthread_mutex_lock(&txn_map_mutex_s);
//    std::map<i64, txn_entry_t *>::iterator it = txn_map_s.find(header.tid);
//    if (it == txn_map_s.end()) {
//        //pthread_mutex_unlock(&txn_map_mutex_s);
//        verify(0);
//        return NULL; //FIXME
//    }
//    else {
//        PieceStatus *ret = NULL;
//        //pthread_mutex_lock(&it->second->piece_map_mutex_);
//        std::unordered_map<i64, PieceStatus *>::iterator pit
//            = it->second->piece_map_.find(header.pid);
//        if (pit == it->second->piece_map_.end()) {
//            if (insert) {
//                ret = new PieceStatus;
//                it->second->piece_map_[header.pid] = ret;
//            }
//        }
//        else {
//            ret = pit->second;
//        }
//        //pthread_mutex_unlock(&it->second->piece_map_mutex_);
//        //pthread_mutex_unlock(&txn_map_mutex_s);
//        return ret;
//    }
//}
//
//txn_entry_t *TxnRunner::get_txn_entry(const RequestHeader &header) {
//    if (running_mode_s == MODE_NONE
//     || running_mode_s == MODE_DEPTRAN) {
//        if (txn_map_s.empty()) {
//            txn_entry_t *new_entry = new txn_entry_t;
//            txn_map_s[0] = new_entry;
//            new_entry->txn = mdb_txn_mgr_->start(0);
//        }
//        return txn_map_s.begin()->second;
//    }
//    else {
//        txn_entry_t *txn_entry_ret = NULL;
//        //if (running_mode_s == MODE_2PL)
//        //    pthread_mutex_lock(&txn_map_mutex_s);
//
//        std::map<i64, txn_entry_t *>::iterator it = txn_map_s.find(header.tid);
//        if (it == txn_map_s.end()) {
//            txn_entry_ret = new txn_entry_t;
//            mdb::Txn *txn = mdb_txn_mgr_->start(header.tid);
//            txn_entry_ret->txn = txn;
//            //XXX using occ lazy mode: increment version at commit time
//            if (running_mode_s == MODE_OCC) {
//                ((mdb::TxnOCC *)txn)->set_policy(mdb::OCC_LAZY);
//            }
//            else if (running_mode_s == MODE_2PL) {
//                txn_entry_ret->init_2pl();
//            }
//            std::pair<std::map<i64, txn_entry_t *>::iterator, bool> ret
//                = txn_map_s.insert(std::pair<i64, txn_entry_t *>(header.tid,
//                            txn_entry_ret));
//            verify(ret.second);
//        }
//        else {
//            txn_entry_ret = it->second;
//        }
//        //if (running_mode_s == MODE_2PL)
//        //    pthread_mutex_unlock(&txn_map_mutex_s);
//        return txn_entry_ret;
//    }
//}
//
//mdb::Txn *TxnRunner::get_txn(const RequestHeader &header) {
//    if (running_mode_s == MODE_NONE
//     || running_mode_s == MODE_DEPTRAN) {
//        if (txn_map_s.empty()) {
//            txn_entry_t *new_entry = new txn_entry_t;
//            txn_map_s[0] = new_entry;
//            new_entry->txn = mdb_txn_mgr_->start(0);
//        }
//        return txn_map_s.begin()->second->txn;
//    }
//    else {
//        mdb::Txn *txn_ret = NULL;
//        //if (running_mode_s == MODE_2PL)
//        //    pthread_mutex_lock(&txn_map_mutex_s);
//
//        std::map<i64, txn_entry_t *>::iterator it = txn_map_s.find(header.tid);
//        if (it == txn_map_s.end()) {
//            txn_entry_t *new_entry = new txn_entry_t;
//            mdb::Txn *txn = mdb_txn_mgr_->start(header.tid);
//            new_entry->txn = txn;
//            //XXX using occ lazy mode: increment version at commit time
//            if (running_mode_s == MODE_OCC) {
//                ((mdb::TxnOCC *)txn)->set_policy(mdb::OCC_LAZY);
//            }
//            else if (running_mode_s == MODE_2PL) {
//                new_entry->init_2pl();
//            }
//            std::pair<std::map<i64, txn_entry_t *>::iterator, bool> ret
//                = txn_map_s.insert(std::pair<i64, txn_entry_t *>(header.tid,
//                            new_entry));
//            verify(ret.second);
//            txn_ret = txn;
//        }
//        else {
//            txn_ret = it->second->txn;
//        }
//        //if (running_mode_s == MODE_2PL)
//        //    pthread_mutex_unlock(&txn_map_mutex_s);
//        return txn_ret;
//    }
//}

} // namespace deptran
