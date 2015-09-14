#include "all.h"

namespace rococo {

BRQDTxn::BRQDTxn(
        txnid_t tid,
        DTxnMgr *mgr,
        bool ro
) {
//    read_only_ = ro;
//    mdb_txn_ = mgr->get_mdb_txn(tid_);
}

void BRQDTxn::fast_accept(FastAcceptRequest &req, FastAcceptReply *rep, rrr::DeferredReply *defer) {
  ballot_t ballot = req.ballot;
  if (ballot_cmd_seen_ <= ballot &&
        ballot_deps_seen_ <= ballot) {
    ballot_cmd_seen_ = ballot;
    ballot_deps_seen_ = ballot;
    // accept the command.
    cmd_ = req.cmd;
    // choose the deps (fast quorum)
    ballot_deps_vote_ = 0;
    ballot_deps_seen_ = ballot;
    // add into deps. (allocate new vertex)
    // find all interfering commands (transactions)
    // TODO
    // auto pair = TxnRegistry::get(
    //         header.t_type, header.p_type);
    // bool deferred = start_exe_itfr(
    //         pair.defer, pair.txn_handler,
    //         header, input, &res->output);
    // TODO add dep in to reply
    // return the direct dependencies. TODO
    // rep->deps = new SubGraph(this, OPT);
    rep->ack = true;
  } else {
    rep->ack = false;
  }
  if (defer)
    defer->reply();
}

void BRQDTxn::commit(const CommitRequest &req, CommitReply *rep, rrr::DeferredReply *defer) {
    // TODO aggregate graph
  // graph_->aggregate(&req.deps); TODO
  status_ = CMT;
  // TODO check wether need to wait/inquire
//  graph_.wait(this, TCPD, [rep](){this->commit_tcpd(rep);});
}

void BRQDTxn::commit_tcpd(CommitReply *rep) {
  // all predecessors have become COMMITTING
  // TODO graph_ calculate scc, sort, and trigger execute.
  // TODO execute and return results
  // TODO auto pair = TxnRegistry::get(header.t_type, header.p_type);
  //  this->commit_execute();
}

//void BRQDTxn::execute(CommitReply *rep) {
//  // all predecessors
//}

void BRQDTxn::prepare(PrepareReqeust &req, PrepareReply *rep, rrr::DeferredReply *reply) {
  // TODO
  ballot_t ballot = req.ballot;
  if (ballot_cmd_seen_ < ballot &&
          ballot_deps_seen_ < ballot) {
    ballot_cmd_seen_ = ballot;
    ballot_deps_seen_ = ballot;
    rep->ballot_cmd_vote = ballot_cmd_vote_;
    rep->ballot_deps_vote = ballot_deps_vote_;
    rep->cmd = cmd_;
//    rep->deps = deps_;
    rep->ack = true;
  } else {
    rep->ack = false;
  }
}

void BRQDTxn::accept(AcceptRequest& req, AcceptReply *rep, rrr::DeferredReply *defer) {
  if (ballot_cmd_seen_ <= req.ballot &&
        ballot_deps_seen_ <= req.ballot) {
    ballot_cmd_seen_ = req.ballot;
    ballot_deps_seen_ = req.ballot;
    cmd_ = req.cmd;
    rep->ack = true;
  } else {
    rep->ack = false;
  }
}

void BRQDTxn::inquire(InquiryReply *rep, rrr::DeferredReply *defer) {
  // TODO
  //graph_->wait(this, CMT, [rep](){this->inquire_dcpd(req, defer);});
}

void BRQDTxn::inquire_dcpd(InquiryReply *rep, rrr::DeferredReply *defer) {
  //
  if (status_ != BRQDTxn::DCD) {
    //SubGraph* deps = new SubGraph(this, BRQGraph::OPT); TODO
    // rep->deps = deps; // TODO only return those not DECIDED
  } else {
    // SubGraph* deps = new SubGraph(this, BRQGraph::SCC);
    // rep->deps = deps; // TODO only return those in SCC
  }
}
} // namespace rococo
