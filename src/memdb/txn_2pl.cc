
#include <limits>

#include "row.h"
#include "table.h"
#include "txn.h"
#include "txn_2pl.h"
#include "txn_occ.h"

namespace mdb {

//Txn2PL::PieceStatus *Txn2PL::ps_cache_s = nullptr;


query_buf_t& Txn2PL::GetQueryBuf(int64_t pid) {
  verify(0);
//  verify(piece_map_.find(pid) != piece_map_.end());
//  query_buf_t &qb = (pid == ps_cache()->pid_) ? ps_cache()->query_buf_
//                                              : piece_map_[pid]->query_buf_;
//  return qb;
}


Txn2PL::Txn2PL(const TxnMgr *mgr, txn_id_t txnid) :
    Txn(mgr, txnid),
    outcome_(symbol_t::NONE) {

}

Txn2PL::~Txn2PL() {
  verify(this->rtti() == symbol_t::TXN_2PL);
  release_resource();
//  verify(piece_map_.size() == 0);
}

ResultSet Txn2PL::query(Table *tbl,
                        const MultiBlob &mb,
                        bool retrieve,
                        int64_t pid) {
//  query_buf_t &qb = GetQueryBuf(pid);
  if (retrieve) {
    verify(0);
    ResultSet rs = do_query(tbl, mb);
    return rs;
//    Log_debug("query from buf, qb size: %d, pid: %lx, buf addr: %lx",
//              qb.buf.size(), pid, &qb);
//    verify(qb.buf.size() > 0);
//    verify(qb.buf.size() > qb.retrieve_index);
//    ResultSet rs = qb.buf[qb.retrieve_index++];
//    rs.reset();
//    return rs;
  } else {
//    Log_debug("query from table, qb size: %d, pid: %lx, buf addr: %lx",
//              qb.buf.size(), pid, &qb);
    ResultSet rs = do_query(tbl, mb);
//    qb.buf.push_back(rs);
//    verify(qb.buf.size() > 0);
    return rs;
  }
}

void Txn2PL::release_resource() {
  verify(this->rtti() == symbol_t::TXN_2PL);
  updates_.clear();
  inserts_.clear();
  removes_.clear();
}


// insert piece in piece_map_, set reply dragonball & set output
//void Txn2PL::init_piece(i64 tid, i64 pid, rrr::DragonBall *db,
//                        mdb::Value *output,
//                        rrr::i32 *output_size) {
//  PieceStatus *ps = new PieceStatus(tid,
//                                    pid,
//                                    db,
//                                    output,
//                                    output_size,
//                                    &wound_,
//                                    [this]() -> int {
//                                      if (prepared_) { // can't wound
//                                        return 1;
//                                      }
//                                      else {
//                                        wound_ = true;
//                                        return 0;
//                                      }
//                                    },
//                                    this);
//  piece_map_[pid] = ps;
//  SetPsCache(ps);
//}



void Txn2PL::marshal_stage(std::string &str) {
  uint64_t len = str.size();

  // marshal reads_
  uint32_t num_reads = reads_.size();
  str.resize(len + sizeof(num_reads));
  memcpy((void *) (str.data() + len), (void *) &num_reads, sizeof(num_reads));
  len += sizeof(num_reads);
  verify(len == str.size());
  for (auto &it : reads_) {
    MultiBlob mb = it.first->get_key();
    int count = mb.count();
    str.resize(len + sizeof(count));
    memcpy((void *) (str.data() + len), (void *) (&count), sizeof(count));
    len += sizeof(count);
    verify(len == str.size());
    for (int i = 0; i < count; i++) {
      str.resize(len + sizeof(mb[i].len) + mb[i].len);
      memcpy((void *) (str.data() + len),
             (void *) (&(mb[i].len)),
             sizeof(mb[i].len));
      len += sizeof(mb[i].len);
      memcpy((void *) (str.data() + len), (void *) mb[i].data, mb[i].len);
      len += mb[i].len;
    }
    verify(len == str.size());
    str.resize(len + sizeof(it.second));
    memcpy((void *) (str.data() + len),
           (void *) (&it.second),
           sizeof(it.second));
    len += sizeof(it.second);
  }

  // marshal inserts_
  uint32_t num_inserts = inserts_.size();
  str.resize(len + sizeof(num_inserts));
  memcpy((void *) (str.data() + len),
         (void *) &num_inserts,
         sizeof(num_inserts));
  len += sizeof(num_inserts);
  verify(len == str.size());
  for (auto &it : inserts_) {
    it.row->to_string(str);
  }
  len = str.size();

  // marshal updates_
  uint32_t num_updates = updates_.size();
  str.resize(len + sizeof(num_updates));
  memcpy((void *) (str.data() + len),
         (void *) &num_updates,
         sizeof(num_updates));
  len += sizeof(num_updates);
  verify(len == str.size());
  for (auto &it : updates_) {
    MultiBlob mb = it.first->get_key();
    int count = mb.count();
    str.resize(len + sizeof(count));
    memcpy((void *) (str.data() + len), (void *) (&count), sizeof(count));
    len += sizeof(count);
    verify(len == str.size());
    for (int i = 0; i < count; i++) {
      str.resize(len + sizeof(mb[i].len) + mb[i].len);
      memcpy((void *) (str.data() + len),
             (void *) (&(mb[i].len)),
             sizeof(mb[i].len));
      len += sizeof(mb[i].len);
      memcpy((void *) (str.data() + len), (void *) mb[i].data, mb[i].len);
      len += mb[i].len;
    }
    verify(len == str.size());
    str.resize(len + sizeof(it.second.first));
    memcpy((void *) (str.data() + len),
           (void *) (&it.second.first),
           sizeof(it.second.first));
    len += sizeof(it.second.first);
    std::string v_str = to_string(it.second.second);
    uint32_t v_str_len = v_str.size();
    str.resize(len + sizeof(v_str_len) + v_str_len);
    memcpy((void *) (str.data() + len), (void *) &v_str_len, sizeof(v_str_len));
    len += sizeof(v_str_len);
    memcpy((void *) (str.data() + len), (void *) v_str.data(), v_str_len);
    len += v_str_len;
    verify(len == str.size());
  }

  // marshal removes_
  uint32_t num_removes = removes_.size();
  str.resize(len + sizeof(num_removes));
  memcpy((void *) (str.data() + len),
         (void *) &num_removes,
         sizeof(num_removes));
  len += sizeof(num_removes);
  verify(len == str.size());
  for (auto &it : removes_) {
    MultiBlob mb = it.row->get_key();
    int count = mb.count();
    str.resize(len + sizeof(count));
    memcpy((void *) (str.data() + len), (void *) (&count), sizeof(count));
    len += sizeof(count);
    verify(len == str.size());
    for (int i = 0; i < count; i++) {
      str.resize(len + sizeof(mb[i].len) + mb[i].len);
      memcpy((void *) (str.data() + len),
             (void *) (&(mb[i].len)),
             sizeof(mb[i].len));
      len += sizeof(mb[i].len);
      memcpy((void *) (str.data() + len), (void *) mb[i].data, mb[i].len);
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
}

bool Txn2PL::commit() {
  verify(this->rtti() == symbol_t::TXN_2PL);
  // TODO does this verification matter?
//  verify(outcome_ == symbol_t::NONE);
  for (auto &it : inserts_) {
    it.table->insert(it.row);
  }
  for (auto it = updates_.begin(); it != updates_.end(); /* no ++it! */) {
    Row *row = it->first;
    const Table *tbl = row->get_table();
    if (tbl->rtti() == TBL_SNAPSHOT) {
      verify(0);
      // update on snapshot table (remove then insert)
      Row *new_row = row->copy();

      // batch update all values
      auto it_end = updates_.upper_bound(row);
      while (it != it_end) {
        colid_t column_id = it->second.first;
        Value &value = it->second.second;
        new_row->update(column_id, value);
        ++it;
      }

      SnapshotTable *ss_tbl = (SnapshotTable *) tbl;
      ss_tbl->remove(row);
      ss_tbl->insert(new_row);

      //redirect_locks(alocks_, new_row, row);
    } else {
      colid_t column_id = it->second.first;
      Value &value = it->second.second;
      row->update(column_id, value);
      ++it;
    }
  }

  std::vector<Row *> rows_to_remove;
  for (auto &it : removes_) {
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
  return true;
}

bool Txn2PL::read_column(Row *row, colid_t col_id, Value *value) {
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

  *value = row->get_column(col_id);
  insert_into_map(reads_, row, col_id);

  return true;
}

bool Txn2PL::write_column(Row *row, colid_t col_id, const Value &value) {
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

  insert_into_map(updates_, row, std::make_pair(col_id, value));

  return true;
}

bool Txn2PL::insert_row(Table *tbl, Row *row) {
  verify(this->rtti() == symbol_t::TXN_2PL);
  verify(outcome_ == symbol_t::NONE);
  verify(row->get_table() == nullptr);
  inserts_.insert(table_row_pair(tbl, row));
  removes_.erase(table_row_pair(tbl, row));
  return true;
}

bool Txn2PL::remove_row(Table *tbl, Row *row) {
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
    removes_.insert(table_row_pair(tbl, row));
  } else {
    it->row->release();
    inserts_.erase(it);
  }
  updates_.erase(row);

  return true;
}



ResultSet Txn2PL::do_query(Table *tbl, const MultiBlob &mb) {
  MergedCursor *merged_cursor = nullptr;
  KeyOnlySearchRow key_search_row(tbl->schema(), &mb);

  auto inserts_begin =
      inserts_.lower_bound(table_row_pair(tbl, &key_search_row));
  auto inserts_end = inserts_.upper_bound(table_row_pair(tbl, &key_search_row));

  Enumerator<const Row *> *cursor = nullptr;
  if (tbl->rtti() == TBL_UNSORTED) {
    UnsortedTable *t = (UnsortedTable *) tbl;
    cursor = new UnsortedTable::Cursor(t->query(mb));
  } else if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    cursor = new SortedTable::Cursor(t->query(mb));
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    cursor = new SnapshotTable::Cursor(t->query(mb));
  } else {
    verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED
               || tbl->rtti() == TBL_SNAPSHOT);
  }
  merged_cursor =
      new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);

  return ResultSet(merged_cursor);
}


ResultSet Txn2PL::do_query_lt(Table *tbl,
                              const SortedMultiKey &smk,
                              symbol_t order /* =? */) {
  verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC
             || order == symbol_t::ORD_ANY);

  Enumerator<const Row *> *cursor = nullptr;
  if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    cursor = new SortedTable::Cursor(t->query_lt(smk, order));
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    cursor = new SnapshotTable::Cursor(t->query_lt(smk, order));
  } else {
    // range query only works on sorted and snapshot table
    verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
  }

  KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());
  auto inserts_begin =
      inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
  auto inserts_end = inserts_.lower_bound(table_row_pair(tbl, &key_search_row));
  MergedCursor *merged_cursor = nullptr;

  if (order == symbol_t::ORD_DESC) {
    auto inserts_rbegin =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
    auto inserts_rend =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

  } else {
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  }

  return ResultSet(merged_cursor);
}

ResultSet Txn2PL::do_query_gt(Table *tbl,
                              const SortedMultiKey &smk,
                              symbol_t order /* =? */) {
  verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC
             || order == symbol_t::ORD_ANY);

  Enumerator<const Row *> *cursor = nullptr;
  if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    cursor = new SortedTable::Cursor(t->query_gt(smk, order));
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    cursor = new SnapshotTable::Cursor(t->query_gt(smk, order));
  } else {
    // range query only works on sorted and snapshot table
    verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
  }

  KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());
  auto inserts_begin =
      inserts_.upper_bound(table_row_pair(tbl, &key_search_row));
  auto inserts_end =
      inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));
  MergedCursor *merged_cursor = nullptr;

  if (order == symbol_t::ORD_DESC) {
    auto inserts_rbegin =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
    auto inserts_rend =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

  } else {
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  }

  return ResultSet(merged_cursor);
}

ResultSet Txn2PL::do_query_in(Table *tbl,
                              const SortedMultiKey &low,
                              const SortedMultiKey &high,
                              symbol_t order /* =? */) {
  verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC
             || order == symbol_t::ORD_ANY);

  Enumerator<const Row *> *cursor = nullptr;
  if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    cursor = new SortedTable::Cursor(t->query_in(low, high, order));
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    cursor = new SnapshotTable::Cursor(t->query_in(low, high, order));
  } else {
    // range query only works on sorted and snapshot table
    verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
  }

  MergedCursor *merged_cursor = nullptr;
  KeyOnlySearchRow key_search_row_low(tbl->schema(), &low.get_multi_blob());
  KeyOnlySearchRow key_search_row_high(tbl->schema(), &high.get_multi_blob());
  auto inserts_begin =
      inserts_.upper_bound(table_row_pair(tbl, &key_search_row_low));
  auto inserts_end =
      inserts_.lower_bound(table_row_pair(tbl, &key_search_row_high));

  if (order == symbol_t::ORD_DESC) {
    auto inserts_rbegin =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
    auto inserts_rend =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

  } else {
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  }

  return ResultSet(merged_cursor);
}


ResultSet Txn2PL::do_all(Table *tbl, symbol_t order /* =? */) {
  verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC
             || order == symbol_t::ORD_ANY);

  Enumerator<const Row *> *cursor = nullptr;
  if (tbl->rtti() == TBL_UNSORTED) {
    // unsorted tables only accept ORD_ANY
    verify(order == symbol_t::ORD_ANY);
    UnsortedTable *t = (UnsortedTable *) tbl;
    cursor = new UnsortedTable::Cursor(t->all());
  } else if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    cursor = new SortedTable::Cursor(t->all(order));
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    cursor = new SnapshotTable::Cursor(t->all(order));
  } else {
    verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED
               || tbl->rtti() == TBL_SNAPSHOT);
  }

  MergedCursor *merged_cursor = nullptr;
  auto inserts_begin =
      inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
  auto inserts_end =
      inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));

  if (order == symbol_t::ORD_DESC) {
    auto inserts_rbegin =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
    auto inserts_rend =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);
  } else {
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  }

  return ResultSet(merged_cursor);
}

} // namespace mdb
