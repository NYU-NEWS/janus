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
    uint32_t quorum_size_;
    std::map<RowId, uint32_t> accept_counts_;
  public:
    TxnOptionResult(const RecordOptionMap& options, uint32_t quorum_size) :
        quorum_size_(quorum_size) {
      for (auto pair : options) {
        RowId id = pair.first;
        accept_counts_[id] = 0;
      }
    }

    uint32_t AcceptCount(RowId id) { return accept_counts_[id]; }
  };

  struct LeaderContext {
    Ballot ballot;
    std::vector<unique_ptr<OptionSet>> max_tried_;
    unique_ptr<TxnOptionResult> option_results_;

    LeaderContext(LeaderContext&& other) {
      option_results_ = std::move(other.option_results_);
      max_tried_ = std::move(other.max_tried_);
      ballot = other.ballot;
    }

    LeaderContext() {
      // todo: delete once fast path implemented
      ballot.type = CLASSIC;
    }
  };

  struct AcceptorContext {
    Ballot ballot;
    vector<OptionSet> values;

    AcceptorContext() {
      // todo: delete once fast path implemented
      ballot.type = CLASSIC;
    }
  };

  class MdccScheduler : public Scheduler {
  protected:
    std::mutex mtx_;
    MdccCommunicator* communicator_ = nullptr;
    Config* config_ = nullptr;
    uint32_t site_id_ = -1;

    std::map<txnid_t, LeaderContext*> leader_contexts_;
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

    LeaderContext* GetOrCreateLeaderContext(txnid_t txn_id);

    void StartTransaction(
      txnid_t txn_id,
      txntype_t txn_type,
      const map<int32_t, Value> &input,
      i8* result,
      rrr::DeferredReply *defer);

    void init(Config *config, uint32_t site_id);
    void StartPiece(const rococo::SimpleCommand& cmd, int32_t* result, DeferredReply *defer);
    bool LaunchNextPiece(uint64_t txn_id, TxnChopper *chopper);
    void SendUpdateProposal(txnid_t txn_id, const SimpleCommand &cmd, int32_t* result);
    void Phase2aClassic(OptionSet option_set);
    void Phase2bClassic(const Ballot ballot, const std::vector<OptionSet>& values);
    void SetCompatible(const std::vector<OptionSet> &old_options, std::vector<OptionSet> &current_options);
    void Learn(const Ballot& ballot, const vector<OptionSet>& values);

    uint32_t PartitionQourumSize(BallotType type, uint32_t partition_id);
  };
}
