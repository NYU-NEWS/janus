
#include "__dep__.h"
#include "ps.h"
#include "tpl.h"
#include "exec.h"

namespace rococo {


void PieceStatus::start_yes_callback() {
  exec_->SetPsCache(this);
  num_acquired_++;
  verify(num_acquired_ <= num_waiting_);
  if (is_rw_)
    rw_succ_ = true;
  else
    rm_succ_ = true;
}

void PieceStatus::start_no_callback() {
  exec_->SetPsCache(this);
  num_acquired_++;
  rej_ = true;
  verify(num_acquired_ <= num_waiting_);
}


void PieceStatus::reg_rm_lock(mdb::Row *row,
                              const std::function<void(void)> &succ_callback,
                              const std::function<void(void)> &fail_callback) {
  rm_succ_ = false;
  is_rw_ = false;
  verify(row->rtti() == mdb::symbol_t::ROW_FINE);
  mdb::FineLockedRow *fl_row = (mdb::FineLockedRow *) row;
  for (int i = 0; i < row->schema()->columns_count(); i++)
    rm_lock_group_.add(fl_row->get_alock(i), rrr::ALock::WLOCK);
  rm_lock_group_.lock_all(succ_callback, fail_callback);
}

void PieceStatus::reg_rw_lock(const std::vector<mdb::column_lock_t> &col_locks,
                              const std::function<void(void)> &succ_callback,
                              const std::function<void(void)> &fail_callback) {
  rw_succ_ = false;
  is_rw_ = true;
  std::vector<mdb::column_lock_t>::const_iterator it;
  for (it = col_locks.begin(); it != col_locks.end(); it++) {
    verify(it->row->rtti() == mdb::symbol_t::ROW_FINE);
    rw_lock_group_.add(((mdb::FineLockedRow *) it->row)->get_alock(it->column_id),
                       it->type);
  }
  rw_lock_group_.lock_all(succ_callback, fail_callback);
}




} // namespace rococo
