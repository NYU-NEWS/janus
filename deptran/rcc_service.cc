#include "all.h"
#include "scheduler.h"
#include "three_phase/sched.h"

namespace rococo {


RococoServiceImpl::RococoServiceImpl(
    Scheduler *dtxn_mgr,
    ServerControlServiceImpl *scsi
) : scsi_(scsi), dtxn_sched_(dtxn_mgr) {

#ifdef PIECE_COUNT
  piece_count_timer_.start();
  piece_count_prepare_fail_ = 0;
  piece_count_prepare_success_ = 0;
#endif
//  verify(RCCDTxn::dep_s == NULL);
//  RCCDTxn::dep_s = new DepGraph();

  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    // TODO free this
    recorder_ = new Recorder(path);
  }
}

// TODO deprecated
void RococoServiceImpl::do_start_pie(
    const RequestHeader &header,
    const Value *input,
    i32 input_size,
    rrr::i32 *res,
    Value *output,
    i32 *output_size) {
  verify(0);
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
}

// TODO
void RococoServiceImpl::batch_start_pie(
    const BatchRequestHeader &batch_header,
    const std::vector<Value> &input,
    rrr::i32 *res,
    std::vector<Value> *output) {

  verify(0);
  RequestHeader header;
  header.t_type = batch_header.t_type;
  header.cid = batch_header.cid;
  header.tid = batch_header.tid;
  BatchStartArgsHelper bsah_input, bsah_output;
  int ret;
  ret = bsah_input.init_input(&input, batch_header.num_piece);
  verify(ret == 0);
  ret = bsah_output.init_output(output, batch_header.num_piece,
                                batch_header.expected_output_size);
  verify(ret == 0);
  Value const *piece_input;
  i32 input_size;
  i32 output_size;
  *res = SUCCESS;
  while (0 == bsah_input.get_next_input(&header.p_type, &header.pid,
                                        &piece_input, &input_size,
                                        &output_size)) {
    i32 res_buf;
    do_start_pie(header, piece_input, input_size, &res_buf,
                 bsah_output.get_output_ptr(), &output_size);
    ret = bsah_output.put_next_output(res_buf, output_size);
    verify(ret == 0);
    if (res_buf != SUCCESS)
      *res = REJECT;
  }

  bsah_output.done_output();
}

void RococoServiceImpl::naive_batch_start_pie(
    const std::vector<RequestHeader> &headers,
    const std::vector<vector<Value>> &inputs,
    const std::vector<i32> &output_sizes,
    std::vector<i32> *results,
    std::vector<vector<Value>> *outputs,
    rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);

  verify(0);
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
}

void RococoServiceImpl::start_pie(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    const rrr::i32 &output_size,
    rrr::i32 *res,
    std::vector<mdb::Value> *output,
    rrr::DeferredReply *defer
) {
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

  output->resize(output_size);
  // find stored procedure, and run it
  *res = SUCCESS;
  ((ThreePhaseSched*)dtxn_sched_)->
      OnPhaseOneRequest(header, input, output_size, res, output, defer);
}

void RococoServiceImpl::prepare_txn(
    const rrr::i64 &tid,
    const std::vector<i32> &sids,
    rrr::i32 *res,
    rrr::DeferredReply *defer
) {
  std::lock_guard<std::mutex> guard(mtx_);
  auto sched = (ThreePhaseSched*)dtxn_sched_;
  sched->OnPhaseTwoRequest(tid, sids, res, defer);
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

void RococoServiceImpl::commit_txn(
    const rrr::i64 &tid,
    rrr::i32 *res,
    rrr::DeferredReply *defer
) {
  std::lock_guard<std::mutex> guard(mtx_);
  auto sched = (ThreePhaseSched*) dtxn_sched_;
  sched->OnPhaseThreeRequest(tid, SUCCESS, res, defer);
}

void RococoServiceImpl::abort_txn(
    const rrr::i64 &tid,
    rrr::i32 *res,
    rrr::DeferredReply *defer
) {
  Log::debug("get abort_txn: tid: %ld", tid);
  auto sched = (ThreePhaseSched*) dtxn_sched_;
  sched->OnPhaseThreeRequest(tid, REJECT, res, defer);
}

// TODO find a better way to define batch
void RococoServiceImpl::rcc_batch_start_pie(
    const std::vector<RequestHeader> &headers,
    const std::vector<std::vector<Value>> &inputs,
    BatchChopStartResponse *res,
    rrr::DeferredReply *defer) {

  verify(false);
//    verify(IS_MODE_RCC || IS_MODE_RO6);
//    auto txn = (RCCDTxn*) txn_mgr_->get_or_create(headers[0].tid);
//
//    res->is_defers.resize(headers.size());
//    res->outputs.resize(headers.size());
//
//    auto job = [&headers, &inputs, res, defer, this, txn] () {
//        std::lock_guard<std::mutex> guard(mtx_);
//
//        Log::debug("batch req, headers size:%u", headers.size());
//        auto &tid = headers[0].tid;
////    Vertex<TxnInfo> *tv = RCC::dep_s->start_pie_txn(tid);
//
//        for (int i = 0; i < headers.size(); i++) {
//            auto &header = headers[i];
//            auto &input = inputs[i];
//            auto &output = res->outputs[i];
//
//            //    Log::debug("receive start request. txn_id: %llx, pie_id: %llx", header.tid, header.pid);
//
//            bool deferred;
////            txn->start(header, input, &deferred, &output); FIXME this is missing !!!
//            res->is_defers[i] = deferred ? 1 : 0;
//
//        }
//        RCCDTxn::dep_s->sub_txn_graph(tid, res->gra_m);
//        defer->reply();
//
//    };
//    static bool do_record = Config::get_config()->do_logging();
//
//    if (do_record) {
//        rrr::Marshal m;
//        m << headers << inputs;
//
//        recorder_->submit(m, job);
//    } else {
//        job();
//    }
}

void RococoServiceImpl::rcc_start_pie(
    const RequestHeader &header,
    const std::vector<Value> &input,
    ChopStartResponse *res,
    rrr::DeferredReply *defer
) {
//    Log::debug("receive start request. txn_id: %llx, pie_id: %llx", header.tid, header.pid);
  verify(IS_MODE_RCC || IS_MODE_RO6);
  verify(defer);

  std::lock_guard<std::mutex> guard(this->mtx_);
  RCCDTxn *dtxn = (RCCDTxn *) dtxn_sched_->GetOrCreateDTxn(header.tid);
  dtxn->StartLaunch(header, input, res, defer);

  // TODO remove the stat from here.
//    auto sz_sub_gra = RCCDTxn::dep_s->sub_txn_graph(header.tid, res->gra_m);
//    stat_sz_gra_start_.sample(sz_sub_gra);
//    if (IS_MODE_RO6) {
//        stat_ro6_sz_vector_.sample(res->read_only.size());
//    }
}

// equivalent to commit phrase
void RococoServiceImpl::rcc_finish_txn(
    const ChopFinishRequest &req,
    ChopFinishResponse *res,
    rrr::DeferredReply *defer) {
  //Log::debug("receive finish request. txn_id: %llx, graph size: %d", req.txn_id, req.gra.size());
  verify(IS_MODE_RCC || IS_MODE_RO6);
  verify(defer);
  verify(req.gra.size() > 0);

  std::lock_guard<std::mutex> guard(mtx_);
  RCCDTxn *txn = (RCCDTxn *) dtxn_sched_->GetDTxn(req.txn_id);
  txn->commit(req, res, defer);

  stat_sz_gra_commit_.sample(req.gra.size());
}

void RococoServiceImpl::rcc_ask_txn(
    const rrr::i64 &tid,
    CollectFinishResponse *res,
    rrr::DeferredReply *defer
) {
  verify(IS_MODE_RCC || IS_MODE_RO6);
  std::lock_guard<std::mutex> guard(mtx_);
  RCCDTxn *dtxn = (RCCDTxn *) dtxn_sched_->GetDTxn(tid);
  dtxn->inquire(res, defer);
}

void RococoServiceImpl::rcc_ro_start_pie(
    const RequestHeader &header,
    const vector<Value> &input,
    vector<Value> *output,
    rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);
  RCCDTxn *dtxn = (RCCDTxn *) dtxn_sched_->GetOrCreateDTxn(header.tid, true);
  dtxn->start_ro(header, input, *output, defer);
}

void RococoServiceImpl::rpc_null(rrr::DeferredReply *defer) {
  defer->reply();
}

} // namespace rcc
