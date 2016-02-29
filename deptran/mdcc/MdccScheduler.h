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
    uint32_t num_accepted_;
    RecordOptionMap values_;
  public:
    TxnOptionResult(const RecordOptionMap& options, uint32_t quorum_size) :
        quorum_size_(quorum_size), num_accepted_(0), values_(options) {
    }

    void Accept() {
      num_accepted_++;
    }

    bool HasQourum() {
      return num_accepted_ >= quorum_size_;
    }
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

    LeaderContext(siteid_t site_id) : ballot(site_id, 0, FAST){
    }
  };

  struct AcceptorContext {
    Ballot ballot;
    std::map<Ballot, vector<OptionSet>> values;

    AcceptorContext(siteid_t site_id) : ballot(site_id, 0, FAST) {
    }

    const Ballot * HighestBallotWithValue();
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
    MdccScheduler() : Scheduler(MODE_MDCC), acceptor_context_(-1) {}
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

    LeaderContext* GetLeaderContext(txnid_t txn_id);
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
    void Phase2aClassic(const Ballot ballot, OptionSet option_set);
    void Phase2bClassic(const Ballot ballot, const std::vector<OptionSet>& values);
    void SetCompatible(int new_position, std::vector<OptionSet> &options);
    void Learn(const Ballot& ballot, const vector<OptionSet>& values);

    uint32_t PartitionQourumSize(BallotType type, uint32_t partition_id);
    void Phase2bFast(Ballot ballot, const OptionSet &option_set);
  };
}
