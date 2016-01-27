//
// Created by lamont on 1/11/16.
//
#pragma once
#include "deptran/scheduler.h"
#include "deptran/executor.h"
#include "deptran/txn_chopper.h"
#include "communicator.h"
#include "option.h"

namespace mdcc {
  using rococo::Scheduler;
  using rococo::Executor;
  using rococo::TxnChopper;
  using rococo::SimpleCommand;

  struct TxnOptionResult {
    int num_option_sets=0;
    int num_success=0;
    int num_fail=0;
    TxnOptionResult() {};
    TxnOptionResult(int num_option_sets) : num_option_sets(num_option_sets) {}
  };

  class MdccScheduler : public Scheduler {
  protected:
   std::mutex mtx_;
   MdccCommunicator* communicator_ = nullptr;
   Config* config_ = nullptr;
   uint32_t site_id_ = -1;

   std::map<txnid_t, TxnOptionResult> option_results_;

  public:
   MdccScheduler() : Scheduler(MODE_MDCC) {}
   virtual ~MdccScheduler() {
     if (communicator_) {
       delete communicator_;
     }
   }

   MdccCommunicator* GetOrCreateCommunicator() {
     if (communicator_ == nullptr) {
       communicator_ = new MdccCommunicator(config_, site_id_);
     }
     return communicator_;
   }

   void StartTransaction(
        txnid_t txn_id,
        txntype_t txn_type,
        const map<int32_t, Value> &input,
        i8* result,
        rrr::DeferredReply *defer);
   void init(Config *config, uint32_t site_id);
   void StartPiece(const rococo::SimpleCommand& cmd, int32_t* result, DeferredReply *defer);
   bool LaunchNextPiece(uint64_t txn_id, TxnChopper *chopper);
   void SendUpdateProposal(txnid_t txn_id, const SimpleCommand &cmd, int32_t* result, rrr::DeferredReply* defer);
  };
}
