#include "../__dep__.h"
#include "../txn_chopper.h"
#include "../txn_reg.h"
#include "ps.h"
#include "tpl.h"
#include "exec.h"

namespace rococo {

PieceStatus::PieceStatus(const SimpleCommand& cmd,
                         const function<void()>& callback,
                         map<int32_t, Value> *output,
                         const std::function<int(void)> &wound_callback,
                         TplExecutor *exec)
    : pid_(cmd.id_),
      num_waiting_(1),
      num_acquired_(0),
      rej_(false),
      callback_(callback),
      output_buf_(NULL),
      output_(output),
      finish_(false),
      rw_succ_(false),
      rw_lock_group_(swap_bits(cmd.timestamp_), wound_callback),
      rm_succ_(false),
      rm_lock_group_(swap_bits(cmd.timestamp_), wound_callback),
      is_rw_(false),
      exec_(exec) {
  handler_ = exec_->txn_reg_->get(cmd).txn_handler;
}

void PieceStatus::start_yes_callback() {
  exec_->SetPsCache(this);
  num_acquired_++;
  verify(num_acquired_ <= num_waiting_);
  if (is_rw_)
    rw_succ_ = true;
  if (is_rm_)
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
  is_rm_ = true;
  verify(row->rtti() == mdb::symbol_t::ROW_FINE);
  mdb::FineLockedRow *fl_row = (mdb::FineLockedRow *) row;
  for (int i = 0; i < row->schema()->columns_count(); i++) {
    rm_lock_group_.add(fl_row->get_alock(i), rrr::ALock::WLOCK);
  }
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
