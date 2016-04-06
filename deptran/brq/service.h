
#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "brq_srpc.h"


namespace rococo {

class ServerControlServiceImpl;
class Scheduler;
class BrqSched;
class BrqGraph;

class BrqServiceImpl: public BrqService {

 public:
  // For statistics
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

  BrqServiceImpl() = delete;

  BrqServiceImpl(Scheduler *sched,
                 rrr::PollMgr* poll_mgr,
                 ServerControlServiceImpl *scsi = NULL);

//  void rcc_batch_start_pie(
//      const std::vector<RequestHeader> &headers,
//      const std::vector<map<int32_t, Value>> &inputs,
//      BatchChopStartResponse *res,
//      DeferredReply *defer);

  void Dispatch(const SimpleCommand& cmd,
                int32_t* res,
                map<int32_t, Value>* output,
                BrqGraph* graph,
                DeferredReply* defer) override;

  void Commit(const cmdid_t& cmd_id,
              const RccGraph& graph,
              int32_t *res,
              TxnOutput* output,
              DeferredReply* defer) override;


  void Inquire(const cmdid_t &tid,
               BrqGraph* graph,
               DeferredReply *) override;

  void PreAccept(const cmdid_t &txnid,
                 const RccGraph& graph,
                 int32_t* res,
                 RccGraph* res_graph,
                 DeferredReply* defer) override;
//
//  void rcc_start_pie(const SimpleCommand& cmd,
//                     ChopStartResponse *res,
//                     DeferredReply *defer) override;
//
//  void rcc_finish_txn(const ChopFinishRequest &req,
//                      ChopFinishResponse *res,
//                      DeferredReply *) override;

  void rcc_ro_start_pie(const SimpleCommand& cmd,
                        map<int32_t, Value> *output,
                        DeferredReply *reply);

  void RegisterStats();

 private:
  BrqSched* dtxn_sched();
};

} // namespace rcc
