#pragma once

#include "__dep__.h"
#include "rcc_rpc.h"

#define DepTranServiceImpl RococoServiceImpl

namespace rococo {

class ServerControlServiceImpl;
class Scheduler;
class RococoServiceImpl: public RococoService {

 public:
  // For statistics
  AvgStat stat_sz_gra_start_;
  AvgStat stat_sz_gra_commit_;
  AvgStat stat_sz_gra_ask_;
  AvgStat stat_sz_scc_;
  AvgStat stat_n_ask_;
  AvgStat stat_ro6_sz_vector_;

  std::mutex mtx_;
  Recorder *recorder_ = NULL;
  ServerControlServiceImpl *scsi_; // for statistics;
  Scheduler *dtxn_sched_;

  void do_start_pie(const RequestHeader &header,
                    const Value *input,
                    int32_t input_size,
                    int32_t *res,
                    Value *output,
                    int32_t *outupt_size);


 public:
  void rpc_null(DeferredReply *defer);

  void batch_start_pie(const BatchRequestHeader &batch_header,
                       const std::vector<Value> &input,
                       i32 *res,
                       std::vector<Value> *output);

  void naive_batch_start_pie(
      const std::vector<RequestHeader> &header,
      const std::vector<vector<Value>> &input,
      const std::vector<i32> &output_size,
      std::vector<i32> *res,
      std::vector<vector<Value>> *output,
      DeferredReply *defer);

  void start_pie(const RequestHeader &header,
                 const std::vector<Value> &input,
                 const i32 &output_size,
                 i32 *res,
                 map<int32_t, Value> *output,
                 DeferredReply *defer);

  void prepare_txn(const i64 &tid,
                   const std::vector<i32> &sids,
                   i32 *res,
                   DeferredReply *defer);

  void commit_txn(const i64 &tid,
                  i32 *res,
                  DeferredReply *defer);

  void abort_txn(const i64 &tid,
                 i32 *res,
                 DeferredReply *defer);

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

  RococoServiceImpl() = delete;

  RococoServiceImpl(Scheduler *dtxn_mgr, ServerControlServiceImpl *scsi = NULL);

  void rcc_batch_start_pie(
      const std::vector<RequestHeader> &headers,
      const std::vector<std::vector<Value>> &inputs,
      BatchChopStartResponse *res,
      DeferredReply *defer);

  void rcc_start_pie(
      const RequestHeader &header,
      const std::vector<Value> &input,
      ChopStartResponse *res,
      DeferredReply *defer);

  void rcc_finish_txn(
      const ChopFinishRequest &req,
      ChopFinishResponse *res,
      DeferredReply *);

  void rcc_ask_txn(
      const i64 &tid,
      CollectFinishResponse *res,
      DeferredReply *);

  void rcc_ro_start_pie(
      const RequestHeader &header,
      const vector<Value> &input,
      map<int32_t, Value> *output,
      DeferredReply *reply);

  uint64_t n_asking_ = 0;
};

} // namespace rcc

