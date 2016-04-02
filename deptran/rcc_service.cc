#include "__dep__.h"
#include "config.h"
#include "scheduler.h"
#include "command.h"
#include "command_marshaler.h"
#include "rcc_service.h"
#include "classic/sched.h"
#include "benchmark_control_rpc.h"

namespace rococo {

ClassicServiceImpl::ClassicServiceImpl(Scheduler *sched,
                                       rrr::PollMgr* poll_mgr,
                                       ServerControlServiceImpl *scsi)
    : scsi_(scsi), dtxn_sched_(sched) {

#ifdef PIECE_COUNT
  piece_count_timer_.start();
  piece_count_prepare_fail_ = 0;
  piece_count_prepare_success_ = 0;
#endif

  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    recorder_ = new Recorder(path);
    poll_mgr->add(recorder_);
  }

  this->RegisterStats();
}

// TODO deprecated
//void ClassicServiceImpl::do_start_pie(
//    const RequestHeader &header,
//    const Value *input,
//    i32 input_size,
//    rrr::i32 *res,
//    Value *output,
//    i32 *output_size) {
//  verify(0);
//  TPLDTxn *dtxn = (TPLDTxn *) dtxn_sched_->GetOrCreateDTxn(header.tid);
//  *res = SUCCESS;
//  if (IS_MODE_2PL) {
//    dtxn->execute(header, input, input_size,
//                  res, output, output_size);
//  }
//  else if (IS_MODE_NONE) {
//    dtxn->execute(header, input, input_size,
//                  res, output, output_size);
//  }
//  else if (IS_MODE_OCC) {
//    dtxn->execute(header, input, input_size,
//                  res, output, output_size);
//  }
//  else {
//    verify(0);
//  }
//}

// TODO
//void ClasicServiceImpl::batch_start_pie(
//    verify(0);
//    const BatchRequestHeader &batch_header,
//    const std::vector<Value> &input,
//    rrr::i32 *res,
//    std::vector<Value> *output) {
//
//  verify(0);
//  RequestHeader header;
//  header.t_type = batch_header.t_type;
//  header.cid = batch_header.cid;
//  header.tid = batch_header.tid;
//  BatchStartArgsHelper bsah_input, bsah_output;
//  int ret;
//  ret = bsah_input.init_input(&input, batch_header.num_piece);
//  verify(ret == 0);
//  ret = bsah_output.init_output(output, batch_header.num_piece,
//                                batch_header.expected_output_size);
//  verify(ret == 0);
//  Value const *piece_input;
//  i32 input_size;
//  i32 output_size;
//  *res = SUCCESS;
//  while (0 == bsah_input.get_next_input(&header.p_type, &header.pid,
//                                        &piece_input, &input_size,
//                                        &output_size)) {
//    i32 res_buf;
//    do_start_pie(header, piece_input, input_size, &res_buf,
//                 bsah_output.get_output_ptr(), &output_size);
//    ret = bsah_output.put_next_output(res_buf, output_size);
//    verify(ret == 0);
//    if (res_buf != SUCCESS)
//      *res = REJECT;
//  }
//
//  bsah_output.done_output();
//}

//void ClassicServiceImpl::naive_batch_start_pie(
//    const std::vector<RequestHeader> &headers,
//    const std::vector<vector<Value>> &inputs,
//    const std::vector<i32> &output_sizes,
//    std::vector<i32> *results,
//    std::vector<vector<Value>> *outputs,
//    rrr::DeferredReply *defer) {
//  std::lock_guard<std::mutex> guard(mtx_);
//
//  verify(0);
//
//  DragonBall *defer_reply_db = NULL;
//  if (IS_MODE_2PL) {
//    defer_reply_db = new DragonBall(headers.size(), [/*&headers, */defer/*, results*/]() {
//      //for (int i = 0; i < results->size(); i++) {
//      //    Log::debug("tid: %ld, pid: %ld, results[%d]: %d", headers[i].tid, headers[i].pid, i, (*results)[i]);
//      //}
//      defer->reply();
//    });
//  }
//  Log::debug("naive_batch_start_pie: tid: %ld", headers[0].tid);
//  results->resize(headers.size());
//  outputs->resize(headers.size());
//  int num_pieces = headers.size();
//  for (int i = 0; i < num_pieces; i++) {
//    (*outputs)[i].resize(output_sizes[i]);
//    auto dtxn = (TPLDTxn *) dtxn_sched_->GetOrCreateDTxn(headers[i].tid);
//    if (defer_reply_db) {
//      dtxn->pre_execute_2pl(headers[i], inputs[i],
//                            &((*results)[i]), &((*outputs)[i]), defer_reply_db);
//    }
//    else {
//      dtxn->execute(headers[i], inputs[i],
//                    &(*results)[i], &(*outputs)[i]);
//    }
//  }
//  if (!defer_reply_db)
//    defer->reply();
//  Log::debug("still fine");
//}


void ClassicServiceImpl::Dispatch(const SimpleCommand &cmd,
                                 rrr::i32 *res,
                                 map<int32_t, Value> *output,
                                 rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);

#ifdef PIECE_COUNT
  piece_count_key_t piece_count_key =
      (piece_count_key_t){header.t_type, header.p_type};
  std::map<piece_count_key_t, uint64_t>::iterator pc_it =
      piece_count_.find(piece_count_key);

  if (pc_it == piece_count_.end())
      piece_count_[piece_count_key] = 1;
  else
      piece_count_[piece_count_key]++;
  piece_count_tid_.insert(header.tid);
#endif

//  output->resize(output_size);
  // find stored procedure, and run it
  *res = SUCCESS;
  ((ClassicSched *) dtxn_sched_)->OnDispatch(cmd, res, output,
                                             [defer] () {defer->reply();});
}

void ClassicServiceImpl::Prepare(const rrr::i64 &tid,
                                 const std::vector<i32> &sids,
                                 rrr::i32 *res,
                                 rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);
  auto sched = (ClassicSched*)dtxn_sched_;
  sched->OnPrepare(tid, sids, res, [defer] () {defer->reply();});
//  auto *dtxn = (TPLDTxn *) dtxn_sched_->get(tid);
//  dtxn->prepare_launch(sids, res, defer);
// TODO move the stat to somewhere else.
#ifdef PIECE_COUNT
  std::map<piece_count_key_t, uint64_t>::iterator pc_it;
  if (*res != SUCCESS)
      piece_count_prepare_fail_++;
  else
      piece_count_prepare_success_++;
  if (piece_count_timer_.elapsed() >= 5.0) {
      Log::info("PIECE_COUNT: txn served: %u", piece_count_tid_.size());
      Log::info("PIECE_COUNT: prepare success: %llu, failed: %llu",
        piece_count_prepare_success_, piece_count_prepare_fail_);
      for (pc_it = piece_count_.begin(); pc_it != piece_count_.end(); pc_it++)
          Log::info("PIECE_COUNT: t_type: %d, p_type: %d, started: %llu",
            pc_it->first.t_type, pc_it->first.p_type, pc_it->second);
      piece_count_timer_.start();
  }
#endif
}

void ClassicServiceImpl::Commit(const rrr::i64 &tid,
                                rrr::i32 *res,
                                rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);
  auto sched = (ClassicSched*) dtxn_sched_;
  sched->OnCommit(tid, SUCCESS, res, [defer] () {defer->reply();});
}

void ClassicServiceImpl::Abort(const rrr::i64 &tid,
                               rrr::i32 *res,
                               rrr::DeferredReply *defer) {
  Log::debug("get abort_txn: tid: %ld", tid);
  auto sched = (ClassicSched*) dtxn_sched_;
  sched->OnCommit(tid, REJECT, res, [defer] () {defer->reply();});
}


void ClassicServiceImpl::rpc_null(rrr::DeferredReply *defer) {
  defer->reply();
}


void ClassicServiceImpl::RegisterStats() {
  if (scsi_) {
    scsi_->set_recorder(recorder_);
  }
}
} // namespace rcc
