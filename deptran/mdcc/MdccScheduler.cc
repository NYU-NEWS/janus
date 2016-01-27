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
      Log_info("Start sub-command: command site_id is %d %d %d", cmd->GetSiteId(), cmd->type_, cmd->inn_id_);
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
    req.callback_ = [] (TxnReply& reply) {
      Log_info("TxnReq callback!!!!!! %d", reply.n_try_);
    };

    auto chopper = Frame().CreateChopper(req, txn_reg_);
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
    auto dtxn = dynamic_cast<MdccDTxn*>(this->GetDTxn(txn_id));
    assert(dtxn);
    auto mdb_txn = dynamic_cast<Txn2PL*>(this->GetMTxn(txn_id));
    assert(mdb_txn);

    int num_sent = dtxn->UpdateOptions().size();
    option_results_[txn_id] = TxnOptionResult(num_sent);

    Callback<OptionSet> cb = [this, mdb_txn, txn_id, result, defer](const OptionSet& optionSet) {
      auto& option_result = this->option_results_[txn_id];
      if (!optionSet.Accepted()) {
        Log_info("OptionSet not accepted -- abort: txn %ld, table %s, key %zu", txn_id, optionSet.Table().c_str(), multi_value_hasher()(
            optionSet.Key()));
        option_result.num_fail++;
        mdb_txn->abort();
        *result = FAILURE;
        defer->reply();
      } else {
        Log_info("OptionSet accepted: txn %ld, table %s, key %ld", txn_id, optionSet.Table().c_str(), multi_value_hasher()(
            optionSet.Key()));
        option_result.num_success++;
        if (option_result.num_success == option_result.num_option_sets) {
          *result = SUCCESS;
          defer->reply();
        }
      }
    };

    for (auto option_pair : dtxn->UpdateOptions()) {
      auto& option_set = option_pair.second;
      this->communicator_->SendProposal(BallotType::FAST, txn_id, cmd, option_set, cb);
    }
  }
}
