#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "../txn_chopper.h"
#include "../rcc_rpc.h"
#include "exec.h"
#include "sched.h"

namespace rococo {

ClassicExecutor::~ClassicExecutor() {
}

int ClassicExecutor::OnDispatch(const SimpleCommand &cmd,
                                rrr::i32 *res,
                                map<int32_t, Value> *output,
                                const function<void()> &callback) {
  verify(0);
}

int ClassicExecutor::OnDispatch(const vector<SimpleCommand>& cmd,
                                int32_t* res,
                                TxnOutput* output,
                                const function<void()>& callback) {
  verify(0);
}

int ClassicExecutor::PrepareLaunch(const std::vector<i32> &sids,
                                   rrr::i32 *res,
                                   rrr::DeferredReply *defer) {
  verify(phase_ < 2);
  phase_ = 2;
}

bool ClassicExecutor::Prepare() {
  verify(0);
}

int ClassicExecutor::AbortLaunch(rrr::i32 *res,
                                 const function<void()> &callback) {
  *res = this->Abort();
  if (Config::GetConfig()->do_logging()) {
    const char abort_tag = 'a';
    std::string log_s;
    log_s.resize(sizeof(cmd_id_) + sizeof(abort_tag));
    memcpy((void *) log_s.data(), (void *) &cmd_id_, sizeof(cmd_id_));
    memcpy((void *) log_s.data(), (void *) &abort_tag, sizeof(abort_tag));
    recorder_->submit(log_s);
  }
  callback();
  Log_debug("abort finish");
  return 0;
}

int ClassicExecutor::Abort() {
  verify(mdb_txn() != NULL);
  verify(mdb_txn_ == sched_->RemoveMTxn(cmd_id_));
  // TODO fix, might have double delete here.
  mdb_txn_->abort();
  delete mdb_txn_;
  mdb_txn_ = nullptr;
  return SUCCESS;
}

int ClassicExecutor::CommitLaunch(rrr::i32 *res,
                                  const function<void()> &callback) {
  *res = this->Commit();
  if (Config::GetConfig()->do_logging()) {
    const char commit_tag = 'c';
    std::string log_s;
    log_s.resize(sizeof(cmd_id_) + sizeof(commit_tag));
    memcpy((void *) log_s.data(), (void *) &cmd_id_, sizeof(cmd_id_));
    memcpy((void *) log_s.data(), (void *) &commit_tag, sizeof(commit_tag));
    recorder_->submit(log_s);
  }
  callback();
  return 0;
}

int ClassicExecutor::Commit() {
  verify(0);
}

void ClassicExecutor::execute(const SimpleCommand &cmd,
                                 rrr::i32 *res,
                                 map<int32_t, Value> &output,
                                 rrr::i32 *output_size) {
  verify(0);
  txn_reg_->get(cmd).txn_handler(this,
                                 dtxn_,
                                 const_cast<SimpleCommand&>(cmd),
                                 res,
                                 output);
}

void ClassicExecutor::Execute(const SimpleCommand &cmd,
                                 rrr::i32 *res,
                                 map<int32_t, Value> &output) {

  const auto& handler = txn_reg_->get(cmd).txn_handler;
  handler(this,
          dtxn_,
          const_cast<SimpleCommand&>(cmd),
          res,
          output);
}

} // namespace rococo;
