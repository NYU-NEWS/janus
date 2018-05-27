//
// Created by Shuai on 8/25/17.
//

#pragma once

#include "../classic/scheduler.h"

namespace janus {

class TxFebruus;
class SchedulerFebruus : public SchedulerClassic {
 public:
  using SchedulerClassic::SchedulerClassic;
  std::list<shared_ptr<TxFebruus>> queue_;

  virtual bool Guard(Tx &tx, Row *row, int col_id, bool write = true)
  override;
  int OnPreAccept(const txid_t tx_id, uint64_t& ret_timestamp);
  int OnAccept(const txid_t tx_id, uint64_t timestamp, ballot_t ballot);
  int OnCommit(const txid_t tx_id, uint64_t timestamp);

  void UpdateQueue(shared_ptr<TxFebruus> sp_tx);

};

} // namespace janus

