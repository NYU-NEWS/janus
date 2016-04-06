
#include "dtxn.h"

namespace rococo {

BrqDTxn::BrqDTxn(txnid_t txn_id, BrqGraph *graph)
  : txn_id_(txn_id), graph_(graph) {
}
//
//void BrqDTxn::FastAccept(FastAcceptRequest &req, FastAcceptReply *rep, rrr::DeferredReply *defer) {
//  ballot_t ballot = req.ballot;
//  if (ballot_cmd_seen_ <= ballot &&
//        ballot_deps_seen_ <= ballot) {
//    ballot_cmd_seen_ = ballot;
//    ballot_deps_seen_ = ballot;
//    // accept the command.
//    cmd_ = req.cmd;
//    // choose the deps (fast quorum)
//    ballot_deps_vote_ = 0;
//    ballot_deps_seen_ = ballot;
//    // add into deps. (allocate new vertex)
//    // find all interfering commands (transactions)
//    // TODO
//    // auto pair = TxnRegistry::get(
//    //         header.t_type, header.p_type);
//    // bool deferred = start_exe_itfr(
//    //         pair.defer, pair.txn_handler,
//    //         header, input, &res->output);
//    // TODO add dep in to reply
//    // return the direct dependencies. TODO
//    // rep->deps = new SubGraph(this, OPT);
////    graph_->Insert(this);
//    rep->ack = true;
//  } else {
//    rep->ack = false;
//  }
//  if (defer)
//    defer->reply();
//}
//
//void BrqDTxn::Commit(CommitRequest &req, CommitReply *rep, rrr::DeferredReply *defer) {
//  // save stack context
//  commit_stack_.reply = rep;
//  commit_stack_.defer = defer;
//  // Aggregate graph
//  // wait the graph scheduler
//  graph_->Aggregate(req.subgraph);
//}
//
//void BrqDTxn::commit_exec() {
//  // all predecessors have become COMMITTING
//  commit_stack_.reply->output = cmd_.Execute();
//  commit_stack_.defer->reply();
//}
//
////void BRQDTxn::execute(CommitReply *rep) {
////  // all predecessors
////}
//
//void BrqDTxn::Prepare(PrepareReqeust &req, PrepareReply *rep, rrr::DeferredReply *reply) {
//  // TODO
//  ballot_t ballot = req.ballot;
//  if (ballot_cmd_seen_ < ballot &&
//          ballot_deps_seen_ < ballot) {
//    ballot_cmd_seen_ = ballot;
//    ballot_deps_seen_ = ballot;
//    rep->ballot_cmd_vote = ballot_cmd_vote_;
//    rep->ballot_deps_vote = ballot_deps_vote_;
//    rep->cmd = cmd_;
////    rep->deps = deps_;
//    rep->ack = true;
//  } else {
//    rep->ack = false;
//  }
//}
//
//void BrqDTxn::accept(AcceptRequest& req, AcceptReply *rep, rrr::DeferredReply *defer) {
//  if (ballot_cmd_seen_ <= req.ballot &&
//        ballot_deps_seen_ <= req.ballot) {
//    ballot_cmd_seen_ = req.ballot;
//    ballot_deps_seen_ = req.ballot;
//    cmd_ = req.cmd;
//    rep->ack = true;
//  } else {
//    rep->ack = false;
//  }
//}
//
//void BrqDTxn::inquire(InquiryReply *rep, rrr::DeferredReply *defer) {
//  // TODO
//  //graph_->wait(this, CMT, [rep](){this->inquire_dcpd(req, defer);});
//}
//
//void BrqDTxn::inquire_dcpd(InquiryReply *rep, rrr::DeferredReply *defer) {
//  //
//  if (status_ != BrqDTxn::DCD) {
//    //SubGraph* deps = new SubGraph(this, BRQGraph::OPT); TODO
//    // rep->deps = deps; // TODO only return those not DECIDED
//  } else {
//    // SubGraph* deps = new SubGraph(this, BRQGraph::SCC);
//    // rep->deps = deps; // TODO only return those in SCC
//  }
//}
} // namespace rococo
