#include "MdccScheduler.h"
#include "deptran/frame.h"
#include "deptran/txn_chopper.h"
#include "executor.h"
#include "option.h"
#include "MdccDTxn.h"
#include "Ballot.h"

namespace mdcc {
  using rococo::TxnRequest;
  using rococo::TxnReply;
  using rococo::Frame;

  LeaderContext* MdccScheduler::GetOrCreateLeaderContext(txnid_t txn_id) {
    auto it = leader_contexts_.find(txn_id);
    if (it == leader_contexts_.end()) {
      LeaderContext* leaderContext = new LeaderContext();
      leader_contexts_[txn_id] = leaderContext;
      return leaderContext;
    } else {
      return it->second;
    }
  }
  
  bool MdccScheduler::LaunchNextPiece(txnid_t txn_id, rococo::TxnChopper *chopper) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (chopper->HasMoreSubCmdReadyNotOut()) {
      auto cmd = static_cast<rococo::SimpleCommand*>(chopper->GetNextSubCmd());
      cmd->id_ = txn_id;
      Log_info("Start sub-command: command site_id is %d %d %d",
               cmd->SiteId(), cmd->type_, cmd->inn_id_);
      GetOrCreateCommunicator()->SendStartPiece(*cmd);
      return true;
    } else {
      Log_debug("no more subcommands or no sub-commands ready.");
      return false;
    }
  }

  void MdccScheduler::StartTransaction(txnid_t txn_id,
                                       txntype_t txn_type,
                                       const map<int32_t, Value> &inputs,
                                       i8* result,
                                       rrr::DeferredReply *defer) {
    TxnRequest req;
    req.txn_type_ = txn_type;
    req.input_ = inputs;
    req.n_try_ = 0;
    auto chopper = frame_->CreateChopper(req, txn_reg_);
    Log_debug("chopper num pieces %d", chopper->GetNPieceAll());
    do {} while(LaunchNextPiece(txn_id, chopper));
    Log_debug("exit %s", __FUNCTION__);
  }

  void MdccScheduler::init(Config *config, uint32_t site_id) {
    // TODO: get rid of this; move to constructor; also init communicator in constructor
    this->config_ = config;
    this->site_id_ = site_id;
  }

  void MdccScheduler::StartPiece(const rococo::SimpleCommand& cmd, int32_t* result, DeferredReply *defer) {
    auto executor = static_cast<MdccExecutor*>(this->GetOrCreateExecutor(cmd.id_));
    executor->StartPiece(cmd, result);
  }

  uint32_t MdccScheduler::PartitionQourumSize(BallotType type, parid_t partition_id) {
    int partition_size = config_->GetPartitionSize(partition_id);
    if (type == BallotType::CLASSIC) {
      return static_cast<int>(std::floor(partition_size / 2.0) + 1);
    } else {
      return static_cast<int>(std::floor(3 * partition_size / 4.0) + 1);
    }
  }

  void MdccScheduler::SendUpdateProposal(txnid_t txn_id, const SimpleCommand &cmd, int32_t* result) {
    std::lock_guard<std::mutex> l(mtx_);
    auto dtxn = static_cast<MdccDTxn*>(GetDTxn(txn_id));
    auto mdb_txn = dynamic_cast<Txn2PL*>(GetMTxn(txn_id));
    assert(mdb_txn);

    auto update_options = dtxn->UpdateOptions();
    auto leader_context = GetOrCreateLeaderContext(txn_id);
    auto partition_id = config_->SiteById(site_id_).partition_id_;
    const auto ballot_type = leader_context->ballot.type;
    auto quorum_size = PartitionQourumSize(ballot_type, partition_id);
    leader_context->option_results_ = std::unique_ptr<TxnOptionResult>(new TxnOptionResult(update_options, quorum_size));

    for (auto option_pair : dtxn->UpdateOptions()) {
      auto& option_set = option_pair.second;
      GetOrCreateCommunicator()->SendProposal(ballot_type, txn_id, cmd, option_set);
    }
  }

  void MdccScheduler::Phase2aClassic(OptionSet option_set) {
    std::lock_guard<std::mutex> l(mtx_);
    auto txn_id = option_set.TxnId();
    auto& options = option_set.Options();
    Log_info("%s txn_id %ld options %ld", __FUNCTION__, txn_id, options.size());

    auto ptr = std::unique_ptr<OptionSet>(new OptionSet(std::move(option_set)));
    auto leader_context = GetOrCreateLeaderContext(txn_id);
    leader_context->max_tried_.push_back(std::move(ptr));

    Phase2aRequest req;
    req.site_id = site_id_; // this is really for debug
    req.ballot = leader_context->ballot;
    for (auto& update : leader_context->max_tried_) {
      req.values.push_back(*update);
    }
    GetOrCreateCommunicator()->SendPhase2a(req);
  }

  void MdccScheduler::Phase2bClassic(const Ballot ballot, const std::vector<OptionSet> &values) {
    Log_debug("%s at site %d", __FUNCTION__, site_id_);

    if (acceptor_context_.ballot <= ballot) {
      Log_info("%s: higher ballot received %s at site %d", __FUNCTION__, ballot.string().c_str(), site_id_);
      acceptor_context_.ballot = ballot;
      auto old_options = acceptor_context_.values;
      acceptor_context_.values = values;

      SetCompatible(old_options, acceptor_context_.values);

      Phase2bRequest req;
      req.site_id = site_id_;
      req.ballot = acceptor_context_.ballot;
      req.values = acceptor_context_.values;
      GetOrCreateCommunicator()->SendPhase2b(req);
    }
  }

  void MdccScheduler::SetCompatible(const std::vector<OptionSet> &old_options,
                                    std::vector<OptionSet> &current_options) {
    // not sure if this is correct
    Log_debug("%s at site %d", __FUNCTION__, site_id_);
    for (auto& option : current_options) {
      auto it = std::find_if(old_options.begin(), old_options.end(), [&option] (const OptionSet& s) {
        return option.TxnId() == s.TxnId();
      });
      if (it == old_options.end()) {
        // not doing commutative updates for now
        auto executor = static_cast<MdccExecutor*>(this->GetOrCreateExecutor(option.TxnId()));
        auto is_valid_read = executor->ValidRead(option);
        auto is_valid_single = std::find_if(old_options.begin(), old_options.end(), [&option] (const OptionSet& s) {
          return option.Table() == s.Table() && option.Key() == s.Key();
        }) == old_options.end();

        if (is_valid_read && is_valid_single) {
          Log_debug("%s at site %d: option accepted for txn %ld, %s", __FUNCTION__, site_id_, option.TxnId(), option.Table().c_str());
          option.Accept();
        } else {
          Log_debug("%s at site %d: option rejected for txn %ld, %s", __FUNCTION__, site_id_, option.TxnId(), option.Table().c_str());
        }
      }
    }
  }

  void MdccScheduler::Learn(const Ballot& ballot, const vector<OptionSet>& values) {
    Log_debug("%s at site %d, %ld values", __FUNCTION__, site_id_, values.size());

  }

}
