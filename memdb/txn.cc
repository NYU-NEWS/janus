#include <limits>

#include "row.h"
#include "table.h"
#include "txn.h"

using namespace std;

namespace mdb {

Table* Txn::get_table(const std::string& tbl_name) const {
    return mgr_->get_table(tbl_name);
}

SortedTable* Txn::get_sorted_table(const std::string& tbl_name) const {
    return mgr_->get_sorted_table(tbl_name);
}

UnsortedTable* Txn::get_unsorted_table(const std::string& tbl_name) const {
    return mgr_->get_unsorted_table(tbl_name);
}

SnapshotTable* Txn::get_snapshot_table(const std::string& tbl_name) const {
    return mgr_->get_snapshot_table(tbl_name);
}

ResultSet Txn::query(Table* tbl, const Value& kv, bool retrieve, int64_t pid) {
    switch (rtti()) {
        case symbol_t::TXN_2PL:
            return this->query(tbl, kv.get_blob(), retrieve, pid);
        default:
            return this->query(tbl, kv.get_blob());
    }
}

ResultSet Txn::all(Table* tbl, bool retrieve, int64_t pid, symbol_t order) {
    switch (rtti()) {
        case symbol_t::TXN_2PL:
            return all(tbl, retrieve, pid, order);
        default:
            return this->all(tbl, order);
    }
}

ResultSet Txn::query_lt(Table* tbl, const MultiBlob& mb, symbol_t order /* =? */) {
    return query_lt(tbl, SortedMultiKey(mb, tbl->schema()), order);
}

ResultSet Txn::query_lt(Table* tbl, const MultiBlob& mb, bool retrieve, int64_t pid, symbol_t order /* =? */) {
    switch (rtti()) {
        case symbol_t::TXN_2PL:
            return ((Txn2PL *)this)->query_lt(tbl, SortedMultiKey(mb, tbl->schema()), retrieve, pid, order);
        default:
            return query_lt(tbl, SortedMultiKey(mb, tbl->schema()), order);
    }
}

ResultSet Txn::query_gt(Table* tbl, const MultiBlob& mb, symbol_t order /* =? */) {
    return query_gt(tbl, SortedMultiKey(mb, tbl->schema()), order);
}

ResultSet Txn::query_gt(Table* tbl, const MultiBlob& mb, bool retrieve, int64_t pid, symbol_t order /* =? */) {
    switch (rtti()) {
        case symbol_t::TXN_2PL:
            return ((Txn2PL *)this)->query_gt(tbl, SortedMultiKey(mb, tbl->schema()), retrieve, pid, order);
        default:
            return query_gt(tbl, SortedMultiKey(mb, tbl->schema()), order);
    }
}

ResultSet Txn::query_in(Table* tbl, const MultiBlob& low, const MultiBlob& high, symbol_t order /* =? */) {
    return query_in(tbl, SortedMultiKey(low, tbl->schema()), SortedMultiKey(high, tbl->schema()), order);
}

ResultSet Txn::query_in(Table* tbl, const MultiBlob& low, const MultiBlob& high, bool retrieve, int64_t pid, symbol_t order /* =? */) {
    switch (rtti()) {
        case symbol_t::TXN_2PL:
            return ((Txn2PL *)this)->query_in(tbl, SortedMultiKey(low, tbl->schema()), SortedMultiKey(high, tbl->schema()), retrieve, pid, order);
        default:
            return query_in(tbl, SortedMultiKey(low, tbl->schema()), SortedMultiKey(high, tbl->schema()), order);
    }
}


#ifdef CONFLICT_COUNT
std::map<const Table *, uint64_t> TxnMgr::vc_conflict_count_;
std::map<const Table *, uint64_t> TxnMgr::rl_conflict_count_;
std::map<const Table *, uint64_t> TxnMgr::wl_conflict_count_;
#endif
Txn* TxnMgr::start_nested(Txn* base) {
    return new TxnNested(this, base);
}

UnsortedTable* TxnMgr::get_unsorted_table(const std::string& tbl_name) const {
    Table* tbl = get_table(tbl_name);
    if (tbl != nullptr) {
        verify(tbl->rtti() == TBL_UNSORTED);
    }
    return (UnsortedTable *) tbl;
}

SortedTable* TxnMgr::get_sorted_table(const std::string& tbl_name) const {
    Table* tbl = get_table(tbl_name);
    if (tbl != nullptr) {
        verify(tbl->rtti() == TBL_SORTED);
    }
    return (SortedTable *) tbl;
}

SnapshotTable* TxnMgr::get_snapshot_table(const std::string& tbl_name) const {
    Table* tbl = get_table(tbl_name);
    if (tbl != nullptr) {
        verify(tbl->rtti() == TBL_SNAPSHOT);
    }
    return (SnapshotTable *) tbl;
}



bool TxnUnsafe::read_column(Row* row, column_id_t col_id, Value* value) {
    *value = row->get_column(col_id);
    // always allowed
    return true;
}

bool TxnUnsafe::write_column(Row* row, column_id_t col_id, const Value& value) {
    row->update(col_id, value);
    // always allowed
    return true;
}

bool TxnUnsafe::insert_row(Table* tbl, Row* row) {
    tbl->insert(row);
    // always allowed
    return true;
}

bool TxnUnsafe::remove_row(Table* tbl, Row* row) {
    tbl->remove(row);
    // always allowed
    return true;
}

ResultSet TxnUnsafe::query(Table* tbl, const MultiBlob& mb) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_UNSORTED) {
        UnsortedTable* t = (UnsortedTable *) tbl;
        UnsortedTable::Cursor* cursor = new UnsortedTable::Cursor(t->query(mb));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query(mb));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query(mb));
        return ResultSet(cursor);
    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}

ResultSet TxnUnsafe::query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_lt(smk, order));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_lt(smk, order));
        return ResultSet(cursor);
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}

ResultSet TxnUnsafe::query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_gt(smk, order));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_gt(smk, order));
        return ResultSet(cursor);
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}

ResultSet TxnUnsafe::query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order /* =? */) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_in(low, high, order));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_in(low, high, order));
        return ResultSet(cursor);
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}


ResultSet TxnUnsafe::all(Table* tbl, symbol_t order /* =? */) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_UNSORTED) {
        // unsorted tables only accept ORD_ANY
        verify(order == symbol_t::ORD_ANY);
        UnsortedTable* t = (UnsortedTable *) tbl;
        UnsortedTable::Cursor* cursor = new UnsortedTable::Cursor(t->all());
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->all(order));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->all(order));
        return ResultSet(cursor);
    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}

bool table_row_pair::operator < (const table_row_pair& o) const {
    if (table != o.table) {
        return table < o.table;
    } else {
        // we use ROW_MIN and ROW_MAX as special markers
        // this helps to get a range query on staged insert set
        if (row == ROW_MIN) {
            return o.row != ROW_MIN;
        } else if (row == ROW_MAX) {
            return false;
        } else if (o.row == ROW_MIN) {
            return false;
        } else if (o.row == ROW_MAX) {
            return row != ROW_MAX;
        }
        return (*row) < (*o.row);
    }
}

Row* table_row_pair::ROW_MIN = (Row *) 0;
Row* table_row_pair::ROW_MAX = (Row *) ~0;

Txn2PL::PieceStatus *Txn2PL::ps_cache_ = NULL;

Txn2PL::~Txn2PL() {
    verify(this->rtti() == symbol_t::TXN_2PL);
    release_resource();
    verify(piece_map_.size() == 0);
}

void Txn2PL::release_piece_map(bool commit) {
    verify(piece_map_.size() != 0);
    if (commit) {
        for(auto &it : piece_map_) {
            it.second->commit();
            delete it.second;
        }
        piece_map_.clear();
    }
    else {
        for(auto &it : piece_map_) {
            it.second->abort();
            delete it.second;
        }
        piece_map_.clear();
    }
}

void Txn2PL::release_resource() {
    verify(this->rtti() == symbol_t::TXN_2PL);
    updates_.clear();
    inserts_.clear();
    removes_.clear();


    // unlocking alock group
    //for (auto &it : alock_group_list_) {
    //    it->unlock_all();
    //    delete it;
    //}
    //alock_group_list_.clear();

    // unlocking rm alock group
    //for (auto &it : rm_alock_group_list_) {
    //    verify(it.first->rtti() == ROW_FINE);
    //    it.second->unlock_all();
    //    delete it.second;
    //}
    //rm_alock_group_list_.clear();

    // unlocking
    //for (auto& it : alocks_) {
    //    Row* row = it.first;
    //    if (row->rtti() == ROW_COARSE) {
    //        verify(0);
    //        //assert(it.second == -1);
    //        //((CoarseLockedRow *) row)->unlock_row_by(this->id());
    //    } else if (row->rtti() == ROW_FINE) {
    //        column_id_t column_id = it.second.first;
    //        uint64_t lock_req_id = it.second.second;
    //        //((FineLockedRow *) row)->unlock_column_by(column_id, this->id());
    //        ((FineLockedRow *) row)->unlock_column_by(column_id, lock_req_id);
    //    } else {
    //        // row must either be FineLockedRow or CoarseLockedRow
    //        verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
    //    }
    //}
    //alocks_.clear();
}

void Txn2PL::PieceStatus::reg_rm_lock(Row *row,
        const std::function<void(void)> &succ_callback,
        const std::function<void(void)> &fail_callback) {
    rm_succ_ = false;
    is_rw_ = false;
    verify(row->rtti() == symbol_t::ROW_FINE);
    FineLockedRow *fl_row = (FineLockedRow *)row;
    for (int i = 0; i < row->schema()->columns_count(); i++)
        rm_lock_group_.add(fl_row->get_alock(i), rrr::ALock::WLOCK);
    rm_lock_group_.lock_all(succ_callback, fail_callback);
}

void Txn2PL::PieceStatus::reg_rw_lock(const std::vector<column_lock_t> &col_locks,
        const std::function<void(void)> &succ_callback,
        const std::function<void(void)> &fail_callback) {
    rw_succ_ = false;
    is_rw_ = true;
    std::vector<column_lock_t>::const_iterator it;
    for (it = col_locks.begin(); it != col_locks.end(); it++) {
        verify(it->row->rtti() == symbol_t::ROW_FINE);
        rw_lock_group_.add(((FineLockedRow *)it->row)->get_alock(it->column_id),
                it->type);
    }
    rw_lock_group_.lock_all(succ_callback, fail_callback);
}

// insert piece in piece_map_, set reply dragonball & set output
void Txn2PL::init_piece(i64 tid, i64 pid, rrr::DragonBall *db,
        mdb::Value* output, 
        rrr::i32* output_size) {
    PieceStatus *ps = new PieceStatus(tid,
            pid,
            db,
            output,
            output_size,
            &wound_,
            [this] () -> int {
                if (prepared_) { // can't wound
                    return 1;
                }
                else {
                    wound_ = true;
                    return 0;
                }
            });
    piece_map_[pid] = ps;
    ps_cache_ = ps;
}

void Txn2PL::init_piece(i64 tid, i64 pid, rrr::DragonBall *db,
        std::vector<mdb::Value> *output) {
    PieceStatus *ps = new PieceStatus(tid,
            pid,
            db,
            output,
            &wound_,
            [this, tid, pid] () -> int {
                if (prepared_) { // can't wound
                    return 1;
                }
                else {
                    wound_ = true;
                    return 0;
                }
            });
    piece_map_[pid] = ps;
    ps_cache_ = ps;
}

Txn2PL::PieceStatus *Txn2PL::get_piece_status(i64 pid) {
    verify(ps_cache_);
    if (ps_cache_->pid_ != pid) {
        ps_cache_ = piece_map_[pid];
    }
    return ps_cache_;
}

void Txn2PL::marshal_stage(std::string &str) {
    uint64_t len = str.size();

    // marshal reads_
    uint32_t num_reads = reads_.size();
    str.resize(len + sizeof(num_reads));
    memcpy((void *)(str.data() + len), (void *)&num_reads, sizeof(num_reads));
    len += sizeof(num_reads);
    verify(len == str.size());
    for (auto &it : reads_) {
        MultiBlob mb = it.first->get_key();
        int count = mb.count();
        str.resize(len + sizeof(count));
        memcpy((void *)(str.data() + len), (void *)(&count), sizeof(count));
        len += sizeof(count);
        verify(len == str.size());
        for (int i = 0; i < count; i++) {
            str.resize(len + sizeof(mb[i].len) + mb[i].len);
            memcpy((void *)(str.data() + len), (void *)(&(mb[i].len)), sizeof(mb[i].len));
            len += sizeof(mb[i].len);
            memcpy((void *)(str.data() + len), (void *)mb[i].data, mb[i].len);
            len += mb[i].len;
        }
        verify(len == str.size());
        str.resize(len + sizeof(it.second));
        memcpy((void *)(str.data() + len), (void *)(&it.second), sizeof(it.second));
        len += sizeof(it.second);
    }

    // marshal inserts_
    uint32_t num_inserts = inserts_.size();
    str.resize(len + sizeof(num_inserts));
    memcpy((void *)(str.data() + len), (void *)&num_inserts, sizeof(num_inserts));
    len += sizeof(num_inserts);
    verify(len == str.size());
    for (auto &it : inserts_) {
        it.row->to_string(str);
    }
    len = str.size();

    // marshal updates_
    uint32_t num_updates = updates_.size();
    str.resize(len + sizeof(num_updates));
    memcpy((void *)(str.data() + len), (void *)&num_updates, sizeof(num_updates));
    len += sizeof(num_updates);
    verify(len == str.size());
    for (auto &it : updates_) {
        MultiBlob mb = it.first->get_key();
        int count = mb.count();
        str.resize(len + sizeof(count));
        memcpy((void *)(str.data() + len), (void *)(&count), sizeof(count));
        len += sizeof(count);
        verify(len == str.size());
        for (int i = 0; i < count; i++) {
            str.resize(len + sizeof(mb[i].len) + mb[i].len);
            memcpy((void *)(str.data() + len), (void *)(&(mb[i].len)), sizeof(mb[i].len));
            len += sizeof(mb[i].len);
            memcpy((void *)(str.data() + len), (void *)mb[i].data, mb[i].len);
            len += mb[i].len;
        }
        verify(len == str.size());
        str.resize(len + sizeof(it.second.first));
        memcpy((void *)(str.data() + len), (void *)(&it.second.first), sizeof(it.second.first));
        len += sizeof(it.second.first);
        std::string v_str = to_string(it.second.second);
        uint32_t v_str_len = v_str.size();
        str.resize(len + sizeof(v_str_len) + v_str_len);
        memcpy((void *)(str.data() + len), (void *)&v_str_len, sizeof(v_str_len));
        len += sizeof(v_str_len);
        memcpy((void *)(str.data() + len), (void *)v_str.data(), v_str_len);
        len += v_str_len;
        verify(len == str.size());
    }

    // marshal removes_
    uint32_t num_removes = removes_.size();
    str.resize(len + sizeof(num_removes));
    memcpy((void *)(str.data() + len), (void *)&num_removes, sizeof(num_removes));
    len += sizeof(num_removes);
    verify(len == str.size());
    for (auto &it : removes_) {
        MultiBlob mb = it.row->get_key();
        int count = mb.count();
        str.resize(len + sizeof(count));
        memcpy((void *)(str.data() + len), (void *)(&count), sizeof(count));
        len += sizeof(count);
        verify(len == str.size());
        for (int i = 0; i < count; i++) {
            str.resize(len + sizeof(mb[i].len) + mb[i].len);
            memcpy((void *)(str.data() + len), (void *)(&(mb[i].len)), sizeof(mb[i].len));
            len += sizeof(mb[i].len);
            memcpy((void *)(str.data() + len), (void *)mb[i].data, mb[i].len);
            len += mb[i].len;
        }
        verify(len == str.size());
    }
}

void Txn2PL::abort() {
    verify(this->rtti() == symbol_t::TXN_2PL);
    verify(outcome_ == symbol_t::NONE);
    outcome_ = symbol_t::TXN_ABORT;
    release_resource();
    release_piece_map(false/*abort*/);
}

//static void redirect_locks(unordered_multimap<Row*, std::pair<column_id_t, uint64_t>>& locks, Row* new_row, Row* old_row) {
//    auto it_pair = locks.equal_range(old_row);
//    vector<std::pair<column_id_t, uint64_t>> locked_columns;
//    for (auto it_lock = it_pair.first; it_lock != it_pair.second; ++it_lock) {
//        locked_columns.push_back(it_lock->second);
//    }
//    if (!locked_columns.empty()) {
//        locks.erase(old_row);
//    }
//    for (auto& col_id : locked_columns) {
//        insert_into_map(locks, new_row, col_id);
//    }
//}
//
static void redirect_locks(unordered_multimap<Row*, column_id_t>& locks, Row* new_row, Row* old_row) {
    auto it_pair = locks.equal_range(old_row);
    vector<column_id_t> locked_columns;
    for (auto it_lock = it_pair.first; it_lock != it_pair.second; ++it_lock) {
        locked_columns.push_back(it_lock->second);
    }
    if (!locked_columns.empty()) {
        locks.erase(old_row);
    }
    for (auto& col_id : locked_columns) {
        insert_into_map(locks, new_row, col_id);
    }
}

bool Txn2PL::commit() {
    verify(this->rtti() == symbol_t::TXN_2PL);
    verify(outcome_ == symbol_t::NONE);
    for (auto& it : inserts_) {
        it.table->insert(it.row);
    }
    for (auto it = updates_.begin(); it != updates_.end(); /* no ++it! */) {
        Row* row = it->first;
        const Table* tbl = row->get_table();
        if (tbl->rtti() == TBL_SNAPSHOT) {
            verify(0);
            // update on snapshot table (remove then insert)
            Row* new_row = row->copy();

            // batch update all values
            auto it_end = updates_.upper_bound(row);
            while (it != it_end) {
                column_id_t column_id = it->second.first;
                Value& value = it->second.second;
                new_row->update(column_id, value);
                ++it;
            }

            SnapshotTable* ss_tbl = (SnapshotTable *) tbl;
            ss_tbl->remove(row);
            ss_tbl->insert(new_row);

            //redirect_locks(alocks_, new_row, row);
        } else {
            column_id_t column_id = it->second.first;
            Value& value = it->second.second;
            row->update(column_id, value);
            ++it;
        }
    }

    std::vector<Row *> rows_to_remove;
    for (auto& it : removes_) {
        // remove the locks since the row has gone already
        //alocks_.erase(it.row);
        it.table->remove(it.row,
                false/* DO NOT free the row until it's erased from tbl */);
        rows_to_remove.push_back(it.row);
    }
    outcome_ = symbol_t::TXN_COMMIT;
    release_resource();
    for (auto &it : rows_to_remove)
        it->release();
    release_piece_map(true/*commit*/);
    return true;
}

//void Txn2PL::reg_rm_lock_group(Row *row,
//        const std::function<void(void)> &succ_callback,
//        const std::function<void(void)> &fail_callback) {
//    rrr::ALockGroup *alock_grp = new rrr::ALockGroup;
//    std::function<void(void)> yes_callback = [this, row, succ_callback, alock_grp]()
//    {
//        rm_alock_group_list_.insert(std::pair<Row *, rrr::ALockGroup *>(row, alock_grp));
//        succ_callback();
//    };
//    verify(row->rtti() == symbol_t::ROW_FINE);
//    rrr::ALock *alock = ((FineLockedRow *)row)->get_alock(0);
//    for (int i = 0; i < row->schema()->columns_count(); i++)
//        alock_grp->add(*(alock + i), rrr::ALock::WLOCK);
//    alock_grp->lock_all(yes_callback, fail_callback);
//}
//
//void Txn2PL::reg_lock_group(const std::vector<column_lock_t> &col_locks,
//        const std::function<void(void)> &succ_callback,
//        const std::function<void(void)> &fail_callback) {
//    rrr::ALockGroup *alock_grp = new rrr::ALockGroup;
//    std::function<void(void)> yes_callback = [this, succ_callback, alock_grp]()
//    {
//        alock_group_list_.push_back(alock_grp);
//        succ_callback();
//    };
//    std::vector<column_lock_t>::const_iterator it;
//    for (it = col_locks.begin(); it != col_locks.end(); it++) {
//        verify(it->row->rtti() == symbol_t::ROW_FINE);
//        alock_grp->add(*(((FineLockedRow *)it->row)->get_alock(it->column_id)),
//                it->type);
//    }
//    alock_grp->lock_all(yes_callback, fail_callback);
//}
//
//void Txn2PL::reg_read_column(Row *row, column_id_t col_id, std::function<void(void)> succ_callback, std::function<void(void)> fail_callback) {
//    verify(0);
//    verify(row->rtti() == symbol_t::ROW_FINE);
//    FineLockedRow* fine_row = ((FineLockedRow *) row);
//    std::function<void(uint64_t)> yes_callback
//        = [row, col_id, succ_callback, this] (uint64_t lock_req_id) {
//            insert_into_map(alocks_, row,
//                    std::pair<column_id_t, uint64_t>(col_id, lock_req_id));
//            succ_callback();
//        };
//    /*uint64_t lock_req_id = */ // don't need req_id, since no abort happend
//                                // until all locks timeout or acquired
//    fine_row->reg_rlock(col_id, yes_callback, fail_callback);
//}

bool Txn2PL::read_column(Row* row, column_id_t col_id, Value* value) {
    verify(this->rtti() == symbol_t::TXN_2PL);
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just read from staging area
        *value = row->get_column(col_id);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            *value = it->second.second;
            return true;
        }
    }

    // reading from actual table data, needs locking
    //if (row->rtti() == symbol_t::ROW_COARSE) {
    //    CoarseLockedRow* coarse_row = (CoarseLockedRow *) row;
    //    if (!coarse_row->rlock_row_by(this->id())) {
    //        return false;
    //    }
    //    insert_into_map(locks_, row, -1);
    //} else if (row->rtti() == symbol_t::ROW_FINE) {
    //    FineLockedRow* fine_row = ((FineLockedRow *) row);
    //    if (!fine_row->rlock_column_by(col_id, this->id())) {
    //        return false;
    //    }
    //    insert_into_map(locks_, row, col_id);
    //} else {
    //    // row must either be FineLockedRow or CoarseLockedRow
    //    verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
    //}
    *value = row->get_column(col_id);
    insert_into_map(reads_, row, col_id);

    return true;
}

//void Txn2PL::reg_write_column(Row *row, column_id_t col_id, std::function<void(void)> succ_callback, std::function<void(void)> fail_callback) {
//    verify(0);
//    verify(row->rtti() == symbol_t::ROW_FINE);
//    FineLockedRow* fine_row = ((FineLockedRow *) row);
//    std::function<void(uint64_t)> yes_callback
//        = [row, col_id, succ_callback, this] (uint64_t lock_req_id) {
//            insert_into_map(alocks_, row,
//                    std::pair<column_id_t, uint64_t>(col_id, lock_req_id));
//            succ_callback();
//        };
//
//    /*uint64_t lock_req_id = */
//    fine_row->reg_wlock(col_id, yes_callback, fail_callback);
//}

bool Txn2PL::write_column(Row* row, column_id_t col_id, const Value& value) {
    verify(this->rtti() == symbol_t::TXN_2PL);
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just write to staging area
        row->update(col_id, value);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            it->second.second = value;
            return true;
        }
    }

    // update staging area, needs locking
    //if (row->rtti() == symbol_t::ROW_COARSE) {
    //    CoarseLockedRow* coarse_row = (CoarseLockedRow *) row;
    //    if (!coarse_row->wlock_row_by(this->id())) {
    //        return false;
    //    }
    //    insert_into_map(locks_, row, -1);
    //} else if (row->rtti() == symbol_t::ROW_FINE) {
    //    FineLockedRow* fine_row = ((FineLockedRow *) row);
    //    if (!fine_row->wlock_column_by(col_id, this->id())) {
    //        return false;
    //    }
    //    insert_into_map(locks_, row, col_id);
    //} else {
    //    // row must either be FineLockedRow or CoarseLockedRow
    //    verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
    //}
    insert_into_map(updates_, row, make_pair(col_id, value));

    return true;
}

bool Txn2PL::insert_row(Table* tbl, Row* row) {
    verify(this->rtti() == symbol_t::TXN_2PL);
    verify(outcome_ == symbol_t::NONE);
    verify(row->get_table() == nullptr);
    inserts_.insert(table_row_pair(tbl, row));
    removes_.erase(table_row_pair(tbl, row));
    return true;
}

bool Txn2PL::remove_row(Table* tbl, Row* row) {
    verify(this->rtti() == symbol_t::TXN_2PL);
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    // we need to sweep inserts_ to find the Row with exact pointer match
    auto it_pair = inserts_.equal_range(table_row_pair(tbl, row));
    auto it = it_pair.first;
    while (it != it_pair.second) {
        if (it->row == row) {
            break;
        }
        ++it;
    }

    if (it == it_pair.second) {
        // lock whole row, only if row is on real table
        //if (row->rtti() == symbol_t::ROW_COARSE) {
        //    CoarseLockedRow* coarse_row = (CoarseLockedRow *) row;
        //    if (!coarse_row->wlock_row_by(this->id())) {
        //        return false;
        //    }
        //    insert_into_map(locks_, row, -1);
        //} else if (row->rtti() == symbol_t::ROW_FINE) {
        //    FineLockedRow* fine_row = ((FineLockedRow *) row);
        //    for (size_t col_id = 0; col_id < row->schema()->columns_count(); col_id++) {
        //        if (!fine_row->wlock_column_by(col_id, this->id())) {
        //            return false;
        //        }
        //        insert_into_map(locks_, row, col_id);
        //    }
        //} else {
        //    // row must either be FineLockedRow or CoarseLockedRow
        //    verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
        //}
        removes_.insert(table_row_pair(tbl, row));
    } else {
        it->row->release();
        inserts_.erase(it);
    }
    updates_.erase(row);

    return true;
}


// for helping locating in inserts set
class KeyOnlySearchRow: public Row {
    const MultiBlob* mb_;
public:
    KeyOnlySearchRow(const Schema* schema, const MultiBlob* mb): mb_(mb) {
        schema_ = schema;
    }
    virtual MultiBlob get_key() const {
        return *mb_;
    }
};


// merge query result in staging area and real table data
class MergedCursor: public NoCopy, public Enumerator<const Row*> {
    Table* tbl_;
    Enumerator<const Row*>* cursor_;

    bool reverse_order_;
    std::multiset<table_row_pair>::const_iterator inserts_next_, inserts_end_;
    std::multiset<table_row_pair>::const_reverse_iterator r_inserts_next_, r_inserts_end_;

    const std::unordered_set<table_row_pair, table_row_pair::hash>& removes_;

    bool cached_;
    const Row* cached_next_;
    const Row* next_candidate_;

    void insert_reset() {
        //TODO it's OK for the current workload
    }

    bool insert_has_next() {
        if (reverse_order_) {
            return r_inserts_next_ != r_inserts_end_;
        } else {
            return inserts_next_ != inserts_end_;
        }
    }

    const Row* insert_get_next() {
        if (reverse_order_) {
            return r_inserts_next_->row;
        } else {
            return inserts_next_->row;
        }
    }

    void insert_advance_next() {
        if (reverse_order_) {
            ++r_inserts_next_;
        } else {
            ++inserts_next_;
        }
    }

    bool prefetch_next() {
        verify(cached_ == false);

        while (next_candidate_ == nullptr && cursor_->has_next()) {
            next_candidate_ = cursor_->next();

            // check if row has been removeds
            table_row_pair needle(tbl_, const_cast<Row*>(next_candidate_));
            if (removes_.find(needle) != removes_.end()) {
                next_candidate_ = nullptr;
            }
        }

        // check if there's data in inserts_
        if (next_candidate_ == nullptr) {
            if (insert_has_next()) {
                cached_ = true;
                cached_next_ = insert_get_next();
                insert_advance_next();
            }
        } else {
            // next_candidate_ != nullptr
            // check which is next: next_candidate_, or next in inserts_
            cached_ = true;
            if (insert_has_next()) {
                if (next_candidate_ < insert_get_next()) {
                    cached_next_ = next_candidate_;
                    next_candidate_ = nullptr;
                } else {
                    cached_next_ = insert_get_next();
                    insert_advance_next();
                }
            } else {
                cached_next_ = next_candidate_;
                next_candidate_ = nullptr;
            }
        }

        return cached_;
    }

public:
    MergedCursor(Table* tbl,
                 Enumerator<const Row*>* cursor,
                 const std::multiset<table_row_pair>::const_iterator& inserts_begin,
                 const std::multiset<table_row_pair>::const_iterator& inserts_end,
                 const std::unordered_set<table_row_pair, table_row_pair::hash>& removes)
        : tbl_(tbl), cursor_(cursor), reverse_order_(false),
          inserts_next_(inserts_begin), inserts_end_(inserts_end), removes_(removes),
          cached_(false), cached_next_(nullptr), next_candidate_(nullptr) {}

    MergedCursor(Table* tbl,
                 Enumerator<const Row*>* cursor,
                 const std::multiset<table_row_pair>::const_reverse_iterator& inserts_rbegin,
                 const std::multiset<table_row_pair>::const_reverse_iterator& inserts_rend,
                 const std::unordered_set<table_row_pair, table_row_pair::hash>& removes)
        : tbl_(tbl), cursor_(cursor), reverse_order_(true),
          r_inserts_next_(inserts_rbegin), r_inserts_end_(inserts_rend), removes_(removes),
          cached_(false), cached_next_(nullptr), next_candidate_(nullptr) {}

    ~MergedCursor() {
        delete cursor_;
    }

    void reset() {
        insert_reset();
        cursor_->reset();
    }

    bool has_next() {
        if (cached_) {
            return true;
        } else {
            return prefetch_next();
        }
    }

    const Row* next() {
        if (!cached_) {
            verify(prefetch_next());
        }
        cached_ = false;
        return cached_next_;
    }
};


ResultSet Txn2PL::do_query(Table* tbl, const MultiBlob& mb) {
    MergedCursor* merged_cursor = nullptr;
    KeyOnlySearchRow key_search_row(tbl->schema(), &mb);

    auto inserts_begin = inserts_.lower_bound(table_row_pair(tbl, &key_search_row));
    auto inserts_end = inserts_.upper_bound(table_row_pair(tbl, &key_search_row));

    Enumerator<const Row*>* cursor = nullptr;
    if (tbl->rtti() == TBL_UNSORTED) {
        UnsortedTable* t = (UnsortedTable *) tbl;
        cursor = new UnsortedTable::Cursor(t->query(mb));
    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        cursor = new SortedTable::Cursor(t->query(mb));
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        cursor = new SnapshotTable::Cursor(t->query(mb));
    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }
    merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);

    return ResultSet(merged_cursor);
}


ResultSet Txn2PL::do_query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    Enumerator<const Row*>* cursor = nullptr;
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        cursor = new SortedTable::Cursor(t->query_lt(smk, order));
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        cursor = new SnapshotTable::Cursor(t->query_lt(smk, order));
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());
    auto inserts_begin = inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
    auto inserts_end = inserts_.lower_bound(table_row_pair(tbl, &key_search_row));
    MergedCursor* merged_cursor = nullptr;

    if (order == symbol_t::ORD_DESC) {
        auto inserts_rbegin = std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
        auto inserts_rend = std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
        merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

    } else {
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    }

    return ResultSet(merged_cursor);
}

ResultSet Txn2PL::do_query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    Enumerator<const Row*>* cursor = nullptr;
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        cursor = new SortedTable::Cursor(t->query_gt(smk, order));
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        cursor = new SnapshotTable::Cursor(t->query_gt(smk, order));
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());
    auto inserts_begin = inserts_.upper_bound(table_row_pair(tbl, &key_search_row));
    auto inserts_end = inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));
    MergedCursor* merged_cursor = nullptr;

    if (order == symbol_t::ORD_DESC) {
        auto inserts_rbegin = std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
        auto inserts_rend = std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
        merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

    } else {
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    }

    return ResultSet(merged_cursor);
}

ResultSet Txn2PL::do_query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    Enumerator<const Row*>* cursor = nullptr;
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        cursor = new SortedTable::Cursor(t->query_in(low, high, order));
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        cursor = new SnapshotTable::Cursor(t->query_in(low, high, order));
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    MergedCursor* merged_cursor = nullptr;
    KeyOnlySearchRow key_search_row_low(tbl->schema(), &low.get_multi_blob());
    KeyOnlySearchRow key_search_row_high(tbl->schema(), &high.get_multi_blob());
    auto inserts_begin = inserts_.upper_bound(table_row_pair(tbl, &key_search_row_low));
    auto inserts_end = inserts_.lower_bound(table_row_pair(tbl, &key_search_row_high));

    if (order == symbol_t::ORD_DESC) {
        auto inserts_rbegin = std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
        auto inserts_rend = std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
        merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

    } else {
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    }

    return ResultSet(merged_cursor);
}


ResultSet Txn2PL::do_all(Table* tbl, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    Enumerator<const Row*>* cursor = nullptr;
    if (tbl->rtti() == TBL_UNSORTED) {
        // unsorted tables only accept ORD_ANY
        verify(order == symbol_t::ORD_ANY);
        UnsortedTable* t = (UnsortedTable *) tbl;
        cursor = new UnsortedTable::Cursor(t->all());
    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        cursor = new SortedTable::Cursor(t->all(order));
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        cursor = new SnapshotTable::Cursor(t->all(order));
    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    MergedCursor* merged_cursor = nullptr;
    auto inserts_begin = inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
    auto inserts_end = inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));

    if (order == symbol_t::ORD_DESC) {
        auto inserts_rbegin = std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
        auto inserts_rend = std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
        merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);
    } else {
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    }

    return ResultSet(merged_cursor);
}



TxnOCC::TxnOCC(const TxnMgr* mgr, txn_id_t txnid, const std::vector<std::string>& table_names): Txn2PL(mgr, txnid), verified_(false) {
    for (auto& it: table_names) {
        SnapshotTable* tbl = get_snapshot_table(it);
        SnapshotTable* snapshot = tbl->snapshot();
        insert_into_map(snapshots_, it, snapshot);
        snapshot_tables_.insert(snapshot);
    }
}

TxnOCC::~TxnOCC() {
    release_resource();
}

void TxnOCC::incr_row_refcount(Row* r) {
    if (accessed_rows_.find(r) == accessed_rows_.end()) {
        r->ref_copy();
        accessed_rows_.insert(r);
    }
}

bool TxnOCC::version_check() {
    if (is_readonly()) {
        return true;
    }
    return version_check(ver_check_read_) && version_check(ver_check_write_);
}

bool TxnOCC::version_check(const std::unordered_map<row_column_pair, version_t, row_column_pair::hash>& ver_info) {
    for (auto& it : ver_info) {
        Row* row = it.first.row;
        column_id_t col_id = it.first.col_id;
        version_t ver = it.second;
        verify(row->rtti() == ROW_VERSIONED);
        VersionedRow* v_row = (VersionedRow *) row;
        if (v_row->get_column_ver(col_id) != ver) {
#ifdef CONFLICT_COUNT
            const Table *tbl = v_row->get_table();
            auto cc_it = TxnMgr::vc_conflict_count_.find(tbl);
            if (cc_it == TxnMgr::vc_conflict_count_.end())
                TxnMgr::vc_conflict_count_[tbl] = 1;
            else
                cc_it->second++;
#endif
            return false;
        }
    }
    return true;
}

void TxnOCC::release_resource() {
    updates_.clear();
    inserts_.clear();
    removes_.clear();

    for (auto& it : locks_) {
        Row* row = it.first;
        verify(row->rtti() == symbol_t::ROW_VERSIONED);
        VersionedRow* v_row = (VersionedRow *) row;
        v_row->unlock_row_by(this->id());
    }
    locks_.clear();

    ver_check_read_.clear();
    ver_check_write_.clear();

    // release ref copy
    for (auto& it: accessed_rows_) {
        it->release();
    }
    accessed_rows_.clear();

    // release snapshots
    for (auto& it: snapshot_tables_) {
        delete it;
    }
    snapshot_tables_.clear();
    snapshots_.clear();
}

void TxnOCC::abort() {
    verify(outcome_ == symbol_t::NONE);
    outcome_ = symbol_t::TXN_ABORT;
    release_resource();
}


bool TxnOCC::commit() {
    verify(outcome_ == symbol_t::NONE);

    if (!this->version_check()) {
        return false;
    }
    verified_ = true;

    this->commit_confirm();
    return true;
}


bool TxnOCC::commit_prepare() {
    verify(outcome_ == symbol_t::NONE);
    verify(verified_ == false);

    if (!this->version_check()) {
        return false;
    }

    // now lock the commit
    for (auto& it : ver_check_read_) {
        Row* row = it.first.row;
        VersionedRow* v_row = (VersionedRow *) row;
        if (!v_row->rlock_row_by(this->id())) {
#ifdef CONFLICT_COUNT
            const Table *tbl = v_row->get_table();
            auto cc_it = TxnMgr::rl_conflict_count_.find(tbl);
            if (cc_it == TxnMgr::rl_conflict_count_.end())
                TxnMgr::rl_conflict_count_[tbl] = 1;
            else
                cc_it->second++;
#endif
            for (auto& lit : locks_) {
                Row* row = lit.first;
                verify(row->rtti() == symbol_t::ROW_VERSIONED);
                VersionedRow* v_row = (VersionedRow *) row;
                v_row->unlock_row_by(this->id());
            }
            locks_.clear();
            return false;
        }
        insert_into_map(locks_, row, -1);
    }
    for (auto& it : ver_check_write_) {
        Row* row = it.first.row;
        VersionedRow* v_row = (VersionedRow *) row;
        if (!v_row->wlock_row_by(this->id())) {
#ifdef CONFLICT_COUNT
            const Table *tbl = v_row->get_table();
            auto cc_it = TxnMgr::wl_conflict_count_.find(tbl);
            if (cc_it == TxnMgr::wl_conflict_count_.end())
                TxnMgr::wl_conflict_count_[tbl] = 1;
            else
                cc_it->second++;
#endif
            for (auto& lit : locks_) {
                Row* row = lit.first;
                verify(row->rtti() == symbol_t::ROW_VERSIONED);
                VersionedRow* v_row = (VersionedRow *) row;
                v_row->unlock_row_by(this->id());
            }
            locks_.clear();
            return false;
        }
        insert_into_map(locks_, row, -1);
    }

    verified_ = true;
    return true;
}

void TxnOCC::commit_confirm() {
    verify(outcome_ == symbol_t::NONE);
    verify(verified_ == true);

    for (auto& it : inserts_) {
        it.table->insert(it.row);
    }
    for (auto it = updates_.begin(); it != updates_.end(); /* no ++it! */) {
        Row* row = it->first;
        verify(row->rtti() == ROW_VERSIONED);
        VersionedRow* v_row = (VersionedRow *) row;
        const Table* tbl = row->get_table();
        if (tbl->rtti() == TBL_SNAPSHOT) {
            // update on snapshot table (remove then insert)
            Row* new_row = row->copy();
            VersionedRow* v_new_row = (VersionedRow *) new_row;

            // batch update all values
            auto it_end = updates_.upper_bound(row);
            while (it != it_end) {
                column_id_t column_id = it->second.first;
                Value& value = it->second.second;
                new_row->update(column_id, value);
                if (policy_ == symbol_t::OCC_LAZY) {
                    // increase version for both old and new row
                    // so that other Txn will verify fail on old row
                    // and also the version info is passed onto new row
                    v_row->incr_column_ver(column_id);
                    v_new_row->incr_column_ver(column_id);
                }
                ++it;
            }

            SnapshotTable* ss_tbl = (SnapshotTable *) tbl;
            ss_tbl->remove(row);
            ss_tbl->insert(new_row);

            redirect_locks(locks_, new_row, row);

            // redirect the accessed_rows_
            auto it_accessed = accessed_rows_.find(row);
            if (it_accessed != accessed_rows_.end()) {
                (*it_accessed)->release();
                accessed_rows_.erase(it_accessed);
                new_row->ref_copy();
                accessed_rows_.insert(new_row);
            }
        } else {
            column_id_t column_id = it->second.first;
            Value& value = it->second.second;
            row->update(column_id, value);
            if (policy_ == symbol_t::OCC_LAZY) {
                v_row->incr_column_ver(column_id);
            }
            ++it;
        }
    }
    for (auto& it : removes_) {
        if (policy_ == symbol_t::OCC_LAZY) {
            Row* row = it.row;
            verify(row->rtti() == symbol_t::ROW_VERSIONED);
            VersionedRow* v_row = (VersionedRow *) row;
            for (size_t col_id = 0; col_id < v_row->schema()->columns_count(); col_id++) {
                v_row->incr_column_ver(col_id);
            }
        }
        // remove the locks since the row has gone already
        locks_.erase(it.row);
        it.table->remove(it.row);
    }
    outcome_ = symbol_t::TXN_COMMIT;
    release_resource();
}


bool TxnOCC::read_column(Row* row, column_id_t col_id, Value* value) {
    if (is_readonly()) {
        *value = row->get_column(col_id);
        return true;
    }

    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just read from staging area
        *value = row->get_column(col_id);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            *value = it->second.second;
            return true;
        }
    }

    // reading from actual table data, track version
    if (row->rtti() == symbol_t::ROW_VERSIONED) {
        VersionedRow* v_row = (VersionedRow *) row;
        insert_into_map(ver_check_read_, row_column_pair(v_row, col_id), v_row->get_column_ver(col_id));
        // increase row reference count because later we are going to check its version
        incr_row_refcount(row);

    } else {
        verify(row->rtti() == symbol_t::ROW_VERSIONED);
    }
    *value = row->get_column(col_id);
    insert_into_map(reads_, row, col_id);

    return true;
}

bool TxnOCC::write_column(Row* row, column_id_t col_id, const Value& value) {
    verify(!is_readonly());
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just write to staging area
        row->update(col_id, value);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            it->second.second = value;
            return true;
        }
    }

    // update staging area, track version
    if (row->rtti() == symbol_t::ROW_VERSIONED) {
        VersionedRow* v_row = (VersionedRow *) row;
        if (policy_ == symbol_t::OCC_EAGER) {
            v_row->incr_column_ver(col_id);
        }
        insert_into_map(ver_check_write_, row_column_pair(v_row, col_id), v_row->get_column_ver(col_id));
        // increase row reference count because later we are going to check its version
        incr_row_refcount(row);

    } else {
        // row must either be FineLockedRow or CoarseLockedRow
        verify(row->rtti() == symbol_t::ROW_VERSIONED);
    }
    insert_into_map(updates_, row, make_pair(col_id, value));

    return true;
}

bool TxnOCC::insert_row(Table* tbl, Row* row) {
    verify(!is_readonly());
    verify(outcome_ == symbol_t::NONE);
    verify(row->rtti() == symbol_t::ROW_VERSIONED);
    verify(row->get_table() == nullptr);

    // we dont need to incr_row_refcount(row), because it is
    // only problematic for read/write

    inserts_.insert(table_row_pair(tbl, row));
    removes_.erase(table_row_pair(tbl, row));
    return true;
}

bool TxnOCC::remove_row(Table* tbl, Row* row) {
    verify(!is_readonly());
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    // we dont need to incr_row_refcount(row), because it is
    // only problematic for read/write

    // we need to sweep inserts_ to find the Row with exact pointer match
    auto it_pair = inserts_.equal_range(table_row_pair(tbl, row));
    auto it = it_pair.first;
    while (it != it_pair.second) {
        if (it->row == row) {
            break;
        }
        ++it;
    }

    if (it == it_pair.second) {
        if (row->rtti() == symbol_t::ROW_VERSIONED) {
            VersionedRow* v_row = (VersionedRow *) row;

            // remember whole row version
            for (size_t col_id = 0; col_id < v_row->schema()->columns_count(); col_id++) {
                if (policy_ == symbol_t::OCC_EAGER) {
                    v_row->incr_column_ver(col_id);
                }
                insert_into_map(ver_check_write_, row_column_pair(v_row, col_id), v_row->get_column_ver(col_id));
                // increase row reference count because later we are going to check its version
                incr_row_refcount(row);
            }

        } else {
            // row must either be FineLockedRow or CoarseLockedRow
            verify(row->rtti() == symbol_t::ROW_VERSIONED);
        }
        removes_.insert(table_row_pair(tbl, row));
    } else {
        it->row->release();
        inserts_.erase(it);
    }
    updates_.erase(row);

    return true;
}


void TxnNested::abort() {
    verify(outcome_ == symbol_t::NONE);
    outcome_ = symbol_t::TXN_ABORT;
}

bool TxnNested::commit() {
    verify(outcome_ == symbol_t::NONE);
    for (auto& it : inserts_) {
        base_->insert_row(it.table, it.row);
    }
    for (auto& it : updates_) {
        Row* row = it.first;
        column_id_t column_id = it.second.first;
        Value& value = it.second.second;
        base_->write_column(row, column_id, value);
    }
    for (auto& it : removes_) {
        base_->remove_row(it.table, it.row);
    }
    outcome_ = symbol_t::TXN_COMMIT;
    return true;
}

bool TxnNested::read_column(Row* row, column_id_t col_id, Value* value) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    // note that we cannot check `row->get_table() == nullptr` and assume the row
    // could be directly accessed. this checking is ok for top level transaction,
    // but for nested transaction, we need to explicitly check if `row in inserts_`
    if (row_inserts_.find(row) != row_inserts_.end()) {
        // row not inserted into table, just read from staging area
        *value = row->get_column(col_id);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            *value = it->second.second;
            return true;
        }
    }

    // reading from base transaction
    return base_->read_column(row, col_id, value);
}

bool TxnNested::write_column(Row* row, column_id_t col_id, const Value& value) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    // note that we cannot check `row->get_table() == nullptr` and assume the row
    // could be directly accessed. this checking is ok for top level transaction,
    // but for nested transaction, we need to explicitly check if `row in inserts_`
    if (row_inserts_.find(row) != row_inserts_.end()) {
        // row not inserted into table, just write to staging area
        row->update(col_id, value);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            it->second.second = value;
            return true;
        }
    }

    // cache updates
    insert_into_map(updates_, row, make_pair(col_id, value));

    return true;
}

bool TxnNested::insert_row(Table* tbl, Row* row) {
    verify(outcome_ == symbol_t::NONE);
    verify(row->get_table() == nullptr);
    inserts_.insert(table_row_pair(tbl, row));
    row_inserts_.insert(row);
    removes_.erase(table_row_pair(tbl, row));
    return true;
}

bool TxnNested::remove_row(Table* tbl, Row* row) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    // we need to sweep inserts_ to find the Row with exact pointer match
    auto it_pair = inserts_.equal_range(table_row_pair(tbl, row));
    auto it = it_pair.first;
    while (it != it_pair.second) {
        if (it->row == row) {
            break;
        }
        ++it;
    }

    if (it == it_pair.second) {
        removes_.insert(table_row_pair(tbl, row));
    } else {
        it->row->release();
        inserts_.erase(it);
        row_inserts_.erase(row);
    }
    updates_.erase(row);

    return true;
}



ResultSet TxnNested::query(Table* tbl, const MultiBlob& mb) {
    KeyOnlySearchRow key_search_row(tbl->schema(), &mb);

    auto inserts_begin = inserts_.lower_bound(table_row_pair(tbl, &key_search_row));
    auto inserts_end = inserts_.upper_bound(table_row_pair(tbl, &key_search_row));

    Enumerator<const Row*>* cursor = base_->query(tbl, mb).unbox();
    MergedCursor* merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    return ResultSet(merged_cursor);
}


ResultSet TxnNested::query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    Enumerator<const Row*>* cursor = base_->query_lt(tbl, smk, order).unbox();

    KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());
    auto inserts_begin = inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
    auto inserts_end = inserts_.lower_bound(table_row_pair(tbl, &key_search_row));
    MergedCursor* merged_cursor = nullptr;

    if (order == symbol_t::ORD_DESC) {
        auto inserts_rbegin = std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
        auto inserts_rend = std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
        merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

    } else {
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    }

    return ResultSet(merged_cursor);
}

ResultSet TxnNested::query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    Enumerator<const Row*>* cursor = base_->query_gt(tbl, smk, order).unbox();

    KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());
    auto inserts_begin = inserts_.upper_bound(table_row_pair(tbl, &key_search_row));
    auto inserts_end = inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));
    MergedCursor* merged_cursor = nullptr;

    if (order == symbol_t::ORD_DESC) {
        auto inserts_rbegin = std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
        auto inserts_rend = std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
        merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

    } else {
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    }

    return ResultSet(merged_cursor);
}

ResultSet TxnNested::query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    Enumerator<const Row*>* cursor = base_->query_in(tbl, low, high, order).unbox();

    MergedCursor* merged_cursor = nullptr;
    KeyOnlySearchRow key_search_row_low(tbl->schema(), &low.get_multi_blob());
    KeyOnlySearchRow key_search_row_high(tbl->schema(), &high.get_multi_blob());
    auto inserts_begin = inserts_.upper_bound(table_row_pair(tbl, &key_search_row_low));
    auto inserts_end = inserts_.lower_bound(table_row_pair(tbl, &key_search_row_high));

    if (order == symbol_t::ORD_DESC) {
        auto inserts_rbegin = std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
        auto inserts_rend = std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
        merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

    } else {
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    }

    return ResultSet(merged_cursor);
}


ResultSet TxnNested::all(Table* tbl, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    Enumerator<const Row*>* cursor = base_->all(tbl, order).unbox();

    MergedCursor* merged_cursor = nullptr;
    auto inserts_begin = inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
    auto inserts_end = inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));

    if (order == symbol_t::ORD_DESC) {
        auto inserts_rbegin = std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
        auto inserts_rend = std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
        merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);
    } else {
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    }

    return ResultSet(merged_cursor);
}


} // namespace mdb
