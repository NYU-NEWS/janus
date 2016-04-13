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
    size_t min_size_;
    size_t max_size_;
    std::map<const Ballot, std::vector<OptionSet>> quorum_results;
    std::vector<OptionSet*> learned_;
    Callback<std::vector<OptionSet*>> learn_callback_;
  public:
    TxnOptionResult(const RecordOptionMap& options, uint32_t quorum_size) :
        quorum_size_(quorum_size), num_accepted_(0),
        min_size_(std::numeric_limits<std::size_t>::max()), max_size_(0) {
    }

    void Accept(const Ballot& ballot, const std::vector<OptionSet>& values) {
      num_accepted_++;
      quorum_results[ballot] = std::move(values);
      auto sz = values.size();
      if (sz < min_size_) {
        min_size_ = sz;
      }
      if (sz > max_size_) {
        max_size_ = sz;
      }
    }

    bool HasQourum() {
      return num_accepted_ >= quorum_size_;
    }

    bool HasUnlearnedOptionSet() {
      return max_size_ != learned_.size();
    }

    const vector<OptionSet*>& ComputeLearned() {
      // determine the longest common prefix among the option sets
      bool done = false;
      for (size_t p=0; p<min_size_; p++) {
        OptionSet* first = nullptr;
        for (auto it = quorum_results.begin(); it != quorum_results.end(); it++) {
          auto& current_result = (*it).second;
          if (it == quorum_results.begin()) {
            first = &current_result[p];
          }
          if ( !(*first == current_result[p]) ) {
            done = true;
            break;
          }
        }

        if (!done) {
          // all OptionSets have equivalent values at position p -- learn
          learned_.push_back(first);
        }
      }
      return learned_;
    }

    void SetCallback(Callback<std::vector<OptionSet*>> cb) {
      learn_callback_ = cb;
    }

    void TriggerCallback(const std::vector<OptionSet*>& options) {
      learn_callback_(options);
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
      const rococo::TxnWorkspace &input,
      i8* result,
      rrr::DeferredReply *defer);

    void init(Config *config, uint32_t site_id);
    void StartPiece(const rococo::SimpleCommand& cmd, int32_t* result, DeferredReply *defer);

    bool LaunchNextPiece(txnid_t txn_id, rococo::TxnChopper *chopper, i8 *result, rrr::DeferredReply *defer);
    void SendUpdateProposal(txnid_t txn_id, const SimpleCommand &cmd, int32_t *result, DeferredReply *defer);
    void Phase2aClassic(const Ballot ballot, OptionSet option_set);
    void Phase2bClassic(const Ballot ballot, const std::vector<OptionSet>& values);
    void SetCompatible(int new_position, std::vector<OptionSet> &options);
    void Learn(const Ballot &ballot, const vector<OptionSet> &values);

    uint32_t PartitionQourumSize(BallotType type, uint32_t partition_id);
    void Phase2bFast(Ballot ballot, const OptionSet &option_set);

    vector<OptionSet> ComputeLearned(const unique_ptr<TxnOptionResult>& option_results);
  };
}
