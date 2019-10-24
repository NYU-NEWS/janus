#pragma once

#include "__dep__.h"
#include "rcc_rpc.h"

#define DepTranServiceImpl ClassicServiceImpl

namespace janus {

class ServerControlServiceImpl;
class Scheduler;
class SimpleCommand;
class Communicator;
class SchedulerClassic;

class ClassicServiceImpl : public ClassicService {

 public:
  AvgStat stat_sz_gra_start_;
  AvgStat stat_sz_gra_commit_;
  AvgStat stat_sz_gra_ask_;
  AvgStat stat_sz_scc_;
  AvgStat stat_n_ask_;
  AvgStat stat_ro6_sz_vector_;
  uint64_t n_asking_ = 0;

//  std::mutex mtx_;
  Recorder* recorder_{nullptr};
  ServerControlServiceImpl* scsi_; // for statistics;
  Communicator* comm_{nullptr};

  Scheduler* dtxn_sched_;

  Scheduler* dtxn_sched() {
    return dtxn_sched_;
  }

  void rpc_null(DeferredReply* defer) override ;

  void Dispatch(const i64& cmd_id,
                const MarshallDeputy& cmd,
                int32_t* res,
                TxnOutput* output,
                DeferredReply* defer_reply) override;

  void Prepare(const i64& tid,
               const std::vector<i32>& sids,
               i32* res,
               DeferredReply* defer) override;

  void Commit(const i64& tid,
              i32* res,
              DeferredReply* defer) override;

  void Abort(const i64& tid,
             i32* res,
             DeferredReply* defer) override;

  void UpgradeEpoch(const uint32_t& curr_epoch,
                    int32_t* res,
                    DeferredReply* defer) override;

  void TruncateEpoch(const uint32_t& old_epoch,
                     DeferredReply* defer) override;

  void TapirAccept(const txid_t& cmd_id,
                   const ballot_t& ballot,
                   const int32_t& decision,
                   rrr::DeferredReply* defer) override;
  void TapirFastAccept(const txid_t& cmd_id,
                       const vector<SimpleCommand>& txn_cmds,
                       rrr::i32* res,
                       rrr::DeferredReply* defer) override;
  void TapirDecide(const txid_t& cmd_id,
                   const rrr::i32& decision,
                   rrr::DeferredReply* defer) override;

  void MsgString(const string& arg,
                 string* ret,
                 rrr::DeferredReply* defer) override;

  void MsgMarshall(const MarshallDeputy& arg,
                   MarshallDeputy* ret,
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

  ClassicServiceImpl(Scheduler* sched,
                     rrr::PollMgr* poll_mgr,
                     ServerControlServiceImpl* scsi = NULL);

  void RccDispatch(const vector<SimpleCommand>& cmd,
                   int32_t* res,
                   TxnOutput* output,
                   MarshallDeputy* p_md_graph,
                   DeferredReply* defer) override;

  void RccFinish(const txid_t& cmd_id,
                 const MarshallDeputy& md_graph,
                 TxnOutput* output,
                 DeferredReply* defer) override;

  void RccInquire(const epoch_t& epoch,
                  const txid_t& tid,
                  MarshallDeputy* p_md_graph,
                  DeferredReply*) override;

  void RccDispatchRo(const SimpleCommand& cmd,
                     map<int32_t, Value>* output,
                     DeferredReply* reply) override;

  void JanusDispatch(const vector<SimpleCommand>& cmd,
                     int32_t* p_res,
                     TxnOutput* p_output,
                     MarshallDeputy* p_md_res_graph,
                     DeferredReply* p_defer) override;

  void JanusCommit(const txid_t& cmd_id,
                   const rank_t& rank,
                   const MarshallDeputy& graph,
                   int32_t* res,
                   TxnOutput* output,
                   DeferredReply* defer) override;

  void JanusCommitWoGraph(const txid_t& cmd_id,
                          const rank_t& rank,
                          int32_t* res,
                          TxnOutput* output,
                          DeferredReply* defer) override;

  void JanusInquire(const epoch_t& epoch,
                    const txid_t& tid,
                    MarshallDeputy* p_md_graph,
                    DeferredReply*) override;

  void JanusPreAccept(const txid_t& txnid,
                      const rank_t& rank,
                      const vector<SimpleCommand>& cmd,
                      const MarshallDeputy& md_graph,
                      int32_t* res,
                      MarshallDeputy* p_md_res_graph,
                      DeferredReply* defer) override;

  void JanusPreAcceptWoGraph(const txid_t& txnid,
                             const rank_t& rank,
                             const vector<SimpleCommand>& cmd,
                             int32_t* res,
                             MarshallDeputy* res_graph,
                             DeferredReply* defer) override;

  void JanusAccept(const txid_t& txnid,
                   const ballot_t& ballot,
                   const MarshallDeputy& md_graph,
                   int32_t* res,
                   DeferredReply* defer) override;

  void PreAcceptFebruus(const txid_t& tx_id,
                        int32_t* res,
                        uint64_t* timestamp,
                        DeferredReply* defer) override;

  void AcceptFebruus(const txid_t& tx_id,
                     const ballot_t& ballot,
                     const uint64_t& timestamp,
                     int32_t* res,
                     DeferredReply* defer) override;

  void CommitFebruus(const txid_t& tx_id,
                     const uint64_t& timestamp,
                     int32_t* res, DeferredReply* defer) override;
 protected:
  void RegisterStats();
};

} // namespace janus

