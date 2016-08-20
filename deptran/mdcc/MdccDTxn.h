//
// Created by lamont on 1/11/16.
//
#pragma once
#include "deptran/dtxn.h"
#include "deptran/scheduler.h"
#include "deptran/mdcc/MdccScheduler.h"
#include "option.h"

using namespace mdb;
using namespace rococo;

namespace mdcc {
  using rococo::DTxn;
  using rococo::Scheduler;

  class MdccDTxn: public DTxn {
  protected:
    MdccScheduler* mgr_;

    // maps <Table, Key> -> Options
    RecordOptionMap update_options_;

  public:
    MdccDTxn() = delete;
    MdccDTxn(txnid_t tid, Scheduler* mgr) :
        DTxn(0, tid, mgr), mgr_(static_cast<MdccScheduler*>(mgr)) {
      mdb_txn_ = mgr_->GetOrCreateMTxn(tid);
    }
    ~MdccDTxn() { ClearOptions(); }

    void ClearOptions() {
      for (auto& pair : update_options_) {
        delete pair.second;
      }
      update_options_.clear();
    }

    virtual Row* CreateRow(const Schema *schema, const std::vector<Value>
    &values) {
      Log_fatal("implement %s!!", __FUNCTION__);
    }

    virtual bool WriteColumn(Row *row, column_id_t col_id, const Value &value, int hint_flag) override;
    OptionSet* CreateUpdateOption(VersionedRow *row, column_id_t col_id, const Value &value);
    const RecordOptionMap& UpdateOptions() const { return update_options_; };
  };
}
