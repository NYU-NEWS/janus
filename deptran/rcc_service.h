#pragma once

#include "__dep__.h"
#include "rcc_rpc.h"

#define DepTranServiceImpl ClassicServiceImpl

namespace rococo {

class ServerControlServiceImpl;
class Scheduler;
class SimpleCommand;

class ClassicServiceImpl: public ClassicService {

 public:

  std::mutex mtx_;
  Recorder *recorder_ = NULL;
  ServerControlServiceImpl *scsi_; // for statistics;
  Scheduler *dtxn_sched_;

//  void do_start_pie(const RequestHeader &header,
//                    const Value *input,
//                    int32_t input_size,
//                    int32_t *res,
//                    Value *output,
//                    int32_t *outupt_size);


 public:
  void rpc_null(DeferredReply *defer);
//
//  void batch_start_pie(const BatchRequestHeader &batch_header,
//                       const std::vector<Value> &input,
//                       i32 *res,
//                       std::vector<Value> *output);
//
//  void naive_batch_start_pie(
//      const std::vector<RequestHeader> &header,
//      const std::vector<vector<Value>> &input,
//      const std::vector<i32> &output_size,
//      std::vector<i32> *res,
//      std::vector<vector<Value>> *output,
//      DeferredReply *defer);

  void Dispatch(const SimpleCommand &cmd,
                int32_t *res,
                map<int32_t, Value> *output,
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

  protected:
    void RegisterStats();
  };

} // namespace rcc

