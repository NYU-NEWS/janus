#pragma once

#include "__dep__.h"
#include "rcc_rpc.h"

#define DepTranServiceImpl ClassicServiceImpl

namespace rococo {

class ServerControlServiceImpl;
class Scheduler;
class SimpleCommand;
class ClassicSched;


class ClassicServiceImpl: public ClassicService {

 public:
  AvgStat stat_sz_gra_start_;
  AvgStat stat_sz_gra_commit_;
  AvgStat stat_sz_gra_ask_;
  AvgStat stat_sz_scc_;
  AvgStat stat_n_ask_;
  AvgStat stat_ro6_sz_vector_;
  uint64_t n_asking_ = 0;

  std::mutex mtx_;
  Recorder *recorder_ = NULL;
  ServerControlServiceImpl *scsi_; // for statistics;
  Scheduler *dtxn_sched_;

  ClassicSched* dtxn_sched() {
    return (ClassicSched*)dtxn_sched_;
  }

  void rpc_null(DeferredReply *defer);

  void Dispatch(const vector<SimpleCommand> &cmd,
                int32_t *res,
                TxnOutput* output,
                DeferredReply *defer_reply) override;

  void Prepare(const i64 &tid,
               const std::vector<i32> &sids,
               i32 *res,
               DeferredReply *defer) override;

  void Commit(const i64 &tid,
              i32 *res,
              DeferredReply *defer) override;

  void Abort(const i64 &tid,
             i32 *res,
             DeferredReply *defer) override;

  void UpgradeEpoch(const uint32_t& curr_epoch,
                    int32_t *res,
                    DeferredReply* defer) override;

  void TruncateEpoch(const uint32_t& old_epoch,
                     DeferredReply* defer) override;

  void TapirAccept(const cmdid_t& cmd_id,
                   const ballot_t& ballot,
                   const int32_t& decision,
                   rrr::DeferredReply* defer) override;
  void TapirFastAccept(const cmdid_t& cmd_id,
                       const vector<SimpleCommand>& txn_cmds,
                       rrr::i32* res,
                       rrr::DeferredReply* defer) override;
  void TapirDecide(const cmdid_t& cmd_id,
                   const rrr::i32& decision,
                   rrr::DeferredReply* defer) override;

#ifdef PIECE_COUNT
  typedef struct piece_count_key_t{
      i32 t_type;
      i32 p_type;
      bool operator<(const piece_count_key_t &rhs) const {
          if (t_type < rhs.t_type)
              return true;
          else if (t_type == rhs.t_type && p_type < rhs.p_type)
              return true;
          return false;
      }
  } piece_count_key_t;

  std::map<piece_count_key_t, uint64_t> piece_count_;

  std::unordered_set<i64> piece_count_tid_;

  uint64_t piece_count_prepare_fail_, piece_count_prepare_success_;

  base::Timer piece_count_timer_;
#endif

 public:

  ClassicServiceImpl() = delete;

  ClassicServiceImpl(Scheduler *sched,
                     rrr::PollMgr* poll_mgr,
                     ServerControlServiceImpl *scsi = NULL);

  void RccDispatch(const vector<SimpleCommand>& cmd,
                  int32_t* res,
                  TxnOutput* output,
                  RccGraph* graph,
                  DeferredReply* defer) override;

  void RccFinish(const cmdid_t& cmd_id,
                const RccGraph& graph,
                TxnOutput* output,
                DeferredReply* defer) override;


  void RccInquire(const epoch_t& epoch,
                  const cmdid_t &tid,
                  RccGraph* graph,
                  DeferredReply *) override;

  void RccDispatchRo(const SimpleCommand& cmd,
                     map<int32_t, Value> *output,
                     DeferredReply *reply);

  void BrqDispatch(const vector<SimpleCommand>& cmd,
                   int32_t* res,
                   TxnOutput* output,
                   Marshallable* res_graph,
                   DeferredReply* defer) override;

  void BrqCommit(const cmdid_t& cmd_id,
                 const Marshallable& graph,
                 int32_t *res,
                 TxnOutput* output,
                 DeferredReply* defer) override;

  void BrqCommitWoGraph(const cmdid_t& cmd_id,
                        int32_t *res,
                        TxnOutput* output,
                        DeferredReply* defer) override;

  void BrqInquire(const epoch_t& epoch,
                  const cmdid_t &tid,
                  Marshallable* graph,
                  DeferredReply *) override;

  void BrqPreAccept(const cmdid_t &txnid,
                    const vector<SimpleCommand>& cmd,
                    const Marshallable& graph,
                    int32_t* res,
                    Marshallable* res_graph,
                    DeferredReply* defer) override;

  void BrqPreAcceptWoGraph(const cmdid_t& txnid,
                           const vector<SimpleCommand>& cmd,
                           int32_t* res,
                           Marshallable* res_graph,
                           DeferredReply* defer) override;

  void BrqAccept(const cmdid_t &txnid,
                 const ballot_t& ballot,
                 const Marshallable& graph,
                 int32_t* res,
                 DeferredReply* defer) override;
  protected:
    void RegisterStats();
  };

} // namespace rcc

