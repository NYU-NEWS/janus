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

  bool MdccScheduler::LaunchNextPiece(txnid_t txn_id, rococo::TxnChopper *chopper) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (chopper->HasMoreSubCmdReadyNotOut()) {
      auto cmd = static_cast<rococo::SimpleCommand*>(chopper->GetNextSubCmd());
      cmd->id_ = txn_id;
      Log_info("Start sub-command: command site_id is %d %d %d",
               cmd->GetParId(), cmd->type_, cmd->inn_id_);
      Callback<StartPieceResponse> cb =
          [this, txn_id, chopper, cmd] (const StartPieceResponse& response) {
            if (response.result!=SUCCESS) {
              Log_info("piece %d failed.", cmd->inn_id());
            } else {
              Log_info("piece %d success.", cmd->inn_id());
              this->LaunchNextPiece(txn_id, chopper);
            }
          };
      GetOrCreateCommunicator()->SendStartPiece(*cmd, cb);
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
    req.callback_ = [txn_id, result, defer] (TxnReply& reply) {
      Log_info("TxnRequest callback %ld %d %d", txn_id, reply.res_, reply.n_try_);
      *result = reply.res_;
      defer->reply();
    };

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
    executor->StartPiece(cmd, result, defer);
  }

  void MdccScheduler::SendUpdateProposal(txnid_t txn_id, const SimpleCommand &cmd, int32_t* result, rrr::DeferredReply* defer) {
    std::lock_guard<std::mutex> l(mtx_);
    auto dtxn = dynamic_cast<MdccDTxn*>(GetDTxn(txn_id));
    assert(dtxn);
    auto mdb_txn = dynamic_cast<Txn2PL*>(GetMTxn(txn_id));
    assert(mdb_txn);

    Callback<OptionSet> cb = [this, mdb_txn, txn_id, result, defer](const OptionSet& optionSet) {
      std::lock_guard<std::mutex> l(this->mtx_);
      auto it = this->option_results_.find(txn_id);
      if (it != this->option_results_.end()) {
        auto& option_result = (*it).second;
        if (!optionSet.Accepted()) {
          Log_info("OptionSet not accepted -- abort: txn %ld, table %s, key %zu", txn_id, optionSet.Table().c_str(), multi_value_hasher()(
              optionSet.Key()));
          mdb_txn->abort();
          *result = FAILURE;
          this->option_results_.erase(it);
          defer->reply();
        } else {
          Log_info("OptionSet accepted: txn %ld, table %s, key %ld", txn_id, optionSet.Table().c_str(), multi_value_hasher()(
              optionSet.Key()));
          auto status = option_result->Success();
          if (status == TxnOptionResult::S_SUCCESS) {
            *result = SUCCESS;
            this->option_results_.erase(it);
            defer->reply();
          }
        }
      }
    };

    option_results_[txn_id] = std::unique_ptr<TxnOptionResult>(new TxnOptionResult(dtxn->UpdateOptions().size()));
    for (auto option_pair : dtxn->UpdateOptions()) {
      auto& option_set = option_pair.second;
      communicator_->SendProposal(BallotType::CLASSIC, txn_id, cmd, option_set, cb);
    }
  }

  void MdccScheduler::Phase2aClassic(OptionSet option_set) {
    std::lock_guard<std::mutex> l(mtx_);
    auto txn_id = option_set.TxnId();
    auto& options = option_set.Options();
    Log_info("%s txn_id %ld options %ld", __FUNCTION__, txn_id, options.size());

    auto ptr = std::unique_ptr<OptionSet>(new OptionSet(std::move(option_set)));
    leader_context_.max_tried_.push_back(std::move(ptr));

    Phase2aRequest req;
    req.site_id = site_id_; // this is really for debug
    req.ballot = leader_context_.ballot;
    // TODO: make this more efficient
    for (auto& update : leader_context_.max_tried_) {
      req.values.push_back(*update);
    }
    communicator_->SendPhase2a(req);
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
      req.ballot = acceptor_context_.ballot;
      req.values = acceptor_context_.values;
      communicator_->SendPhase2b(req);
    }
  }

  void MdccScheduler::SetCompatible(const std::vector<OptionSet> &old_options,
                                    std::vector<OptionSet> &current_options) {
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
}
