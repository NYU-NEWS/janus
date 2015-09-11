#include "all.h"

namespace rococo {

BRQDTxn::BRQDTxn(
        i64 tid,
        DTxnMgr *mgr,
        bool ro
) : DTxn(tid, mgr) {
    read_only_ = ro;
    mdb_txn_ = mgr->get_mdb_txn(tid_);
}

void BRQDTxn::fast_accept(FastAcceptRequest *req, FastStartReply *rep, rrr::DeferredReply *defer) {
  if (ballot_cmd_seen_ <= ballot &&
        ballot_deps_seen_ <= ballot) {
    ballot_cmd_seen_ = ballot;
    ballot_deps_seen_ = ballot;
    // accept the command.
    cmd_.type_ = header.txn_type_;
    cmd_.input = input;
    // choose the deps (fast quorum)
    ballot_deps_vote_ = (0 << 32) && 0;
    ballot_deps_seen_ = ballot;
    // add into deps. (allocate new vertex)
    // find all interfering commands (transactions)
    auto pair = TxnRegistry::get(
            header.t_type, header.p_type);
    bool deferred = start_exe_itfr(
            pair.defer, pair.txn_handler,
            header, input, &res->output);
    // TODO add dep in to reply
    // return the direct dependencies.
    rep->deps = new SubGraph(this, DIRECT);
    rep->ack = true;
  } else {
    rep->ack = false;
  }
  if (defer)
    defer->reply();
}

void BRQDTxn::commit(
        const CommitRequest &req,
        CommitReply *rep,
        rrr::DeferredReply *defer) {
    // TODO aggregate graph
  graph_->aggregate(req.deps);
  status_ = D_CPTD;
  // TODO check wether need to wait/inquire
  graph_.wait(this, TCPD, [rep](){this->commit_tcpd(rep)});
}

void BRQDTxn::commit_tcpd(CommitReply *rep) {
  // all predecessors have become COMMITTING
  // TODO graph_ calculate scc, sort, and trigger execute.
  // TODO execute and return results
  auto pair = TxnRegistry::get(header.t_type, header.p_type);
  this->commit_execute();
}

void BRQDTxn::execute(CommitReply *rep) {
  // all predecessors
}

void BRQDTxn::prepare(PrepareReqeust *request, PrepareReply *rep, rrr:DeferredReply *reply) {
  // TODO
  if (ballot_cmd_seen_ < ballot &&
          ballot_deps_seen_ < ballot) {
    ballot_cmd_seen_ = ballot;
    ballot_deps_seen_ = ballot;
    rep->ballot_cmd_vote = ballot_cmd_vote_;
    rep->ballot_dep_vote = ballot_cmd_vote_;
    rep->cmd_type = cmd_.type_;
    rep->cmd_input = cmd_.input_;
    rep->dep = dep_;
    rep->ack = true;
  } else {
    rep->ack = false;
  }
}

void BRQDTxn::accept(AcceptRequest& request, AcceptReply *reply, rrr::DeferredReplY *defer) {
  if (ballot_cmd_seen_ <= ballot &&
        ballot_deps_seen_ <= ballot) {
    ballot_cmd_seen_ = ballot;
    ballot_deps_seen_ = ballot;
    cmd_.type = header.txn_id;
    cmd_.input = input;
    status_.cmd = ACCEPTED;
    status_.deps = ACCEPTED;
    rep->ack = true;
  } else {
    rep->ack = false;
  }
}

void BRQDTxn::inquire(InquiryReply *rep, rrr::DeferredReply *defer) {
  graph_->wait(this, CMT, [rep](){this->inquire_dcpd(req, defer)};
}

void BRQDTxn::inquire_dcpd(InquiryReply *rep, rrr::DeferredReply *defer) {
  //
  if (status_.itfr < DECIDED) {
    SubGraph* deps = new SubGraph(this, UNDECIDED);
    rep->deps = deps; // TODO only return those not DECIDED
  } else {
    SubGraph* deps = new SubGraph(this, SCC);
    rep->deps = deps; // TODO only return those in SCC
  }
}
} // namespace rococo
