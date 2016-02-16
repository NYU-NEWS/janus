//
// Created by lamont on 1/11/16.
//
#pragma once
#include <memory>
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

  class TxnOptionResult {
  protected:
    int num_option_sets;
    int success_cnt;

  public:
    TxnOptionResult() : TxnOptionResult(0) {};
    TxnOptionResult(int num_option_sets) : num_option_sets(num_option_sets),
                                           success_cnt(0) {
    }

    enum Status {S_SUCCESS, S_ONGOING};
    Status Success() {
      int cnt = ++success_cnt;
      return (cnt < num_option_sets) ?
        Status::S_ONGOING : Status::S_SUCCESS;
    }
  };

  struct LeaderContext {
    Ballot ballot;
    std::vector<unique_ptr<OptionSet>> max_tried_;
  };

  struct AcceptorContext {
    Ballot ballot;
    vector<OptionSet> values;
  };

  class MdccScheduler : public Scheduler {
  protected:
   std::mutex mtx_;
   MdccCommunicator* communicator_ = nullptr;
   Config* config_ = nullptr;
   uint32_t site_id_ = -1;

   std::map<txnid_t, unique_ptr<TxnOptionResult>> option_results_;
   LeaderContext leader_context_;
   AcceptorContext acceptor_context_;
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
   void Phase2aClassic(OptionSet option_set);
   void Phase2bClassic(const Ballot ballot, const std::vector<OptionSet>& values);

    void SetCompatible(const std::vector<OptionSet> &old_options, std::vector<OptionSet> &current_options);
  };
}
