#pragma once

#include "__dep__.h"
#include "exec.h"

namespace rococo {

class TplExecutor;

class PieceStatus {
  friend class Txn2PL;
  //FIXME when locking thread starts to process a piece, and main thread starts to abort one txn
 public:
  static int64_t swap_bits(int64_t num) {
    int64_t ret = num << 32;
    ret |= (num >> 32) & 0x00000000FFFFFFFF;
    return ret;
  }
  i64 pid_;
  int num_waiting_;
  int num_acquired_;
  bool rej_;
  function<void()> callback_;
  mdb::Value *output_buf_;
//  rrr::i32 *output_size_buf_;
  std::map <int32_t, Value> *output_;
  bool finish_;

  bool rw_succ_;
  rrr::ALockGroup rw_lock_group_;
  bool rm_succ_;
  rrr::ALockGroup rm_lock_group_;
  bool is_rw_;
  bool is_rm_ = false;
  TxnHandler handler_;

  TplExecutor *exec_;

  PieceStatus() : rw_lock_group_(0), rm_lock_group_(0) {
    verify(0);
  }

  PieceStatus(const PieceStatus &ps) : rw_lock_group_(0),
                                       rm_lock_group_(0) {
    verify(0);
  }

 public:

  PieceStatus(const SimpleCommand& cmd,
              const function<void()>& callback,
              std::map <int32_t, mdb::Value> *output,
              const std::function<int(void)> &wound_callback,
              TplExecutor *exec);

  ~PieceStatus() {
  }

  bool is_rejected() {
    return rej_ || exec_->wounded_;
  }

  void reg_rm_lock(mdb::Row *row,
                   const std::function<void(void)> &succ_callback,
                   const std::function<void(void)> &fail_callback);

  void reg_rw_lock(const std::vector <mdb::column_lock_t> &col_locks,
                   const std::function<void(void)> &succ_callback,
                   const std::function<void(void)> &fail_callback);

  void abort() {
    verify(finish_);
    if (rw_succ_)
      rw_lock_group_.unlock_all();
    if (rm_succ_)
      rm_lock_group_.unlock_all();
  }

  void commit() {
    verify(finish_);
    if (rw_succ_)
      rw_lock_group_.unlock_all();
  }

  void remove_output() {
  }

  void set_num_waiting_locks(int num) {
    //TODO (right now, only supporting only either rw or delete, not both)
    verify(num <= 1);
    num_waiting_ = num;
    num_acquired_ = 0;
  }

  void start_yes_callback();
  void start_no_callback();

  void get_output(map<int32_t, Value> **output_map,
                  mdb::Value **output,
                  rrr::i32 **output_size) {
    *output_map = output_;
    verify(output_buf_ == nullptr);
    *output = output_buf_;
  }

  bool can_proceed() {
    return true;
  }

  void set_finish() {
    verify(!finish_);
    finish_ = true;
  }

};

} // namespace rococo