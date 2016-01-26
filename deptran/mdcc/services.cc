//
// Created by lamont on 12/17/15.
//
#include "services.h"
#include <deptran/frame.h>
#include "marshal-value.h"
#include "mdcc_rpc.h"
#include "deptran/scheduler.h"
#include "deptran/executor.h"

namespace mdcc {
void MdccClientServiceImpl::Start(const StartRequest& req, StartResponse* res, rrr::DeferredReply* defer) {
  // Server-Side Transaction entry point
  Log_info("%s at site %d", __FUNCTION__, my_site_info_.id);
  dtxn_mgr_->StartTransaction(req.txn_id, req.txn_type, req.inputs, &res->result, defer);
}

void MdccClientServiceImpl::StartPiece(const SimpleCommand& cmd, StartPieceResponse* res, rrr::DeferredReply* defer) {
  Log_info("%s %d:%d at site %d", __FUNCTION__, cmd.type_, cmd.inn_id_, my_site_info_.id);
  dtxn_mgr_->StartPiece(cmd, &res->result, defer);
}

void MdccLearnerService::Learn(const Result& r) {
}

void MdccLeaderService::Propose(const ProposeRequest &req, ProposeResponse *res,
                                rrr::DeferredReply *defer) {
}
void MdccLeaderService::Recover() { }

void MdccAcceptorService::Propose(const ProposeRequest &req,
                                  ProposeResponse *res, rrr::DeferredReply *defer) {
}

void MdccAcceptorService::ProposeFast(const ProposeRequest &req,
                                      ProposeResponse *res, rrr::DeferredReply *defer) {
}

void MdccAcceptorService::Decide(const Result &result,
                                 rrr::DeferredReply *defer) {
}

void MdccAcceptorService::Accept(const AcceptRequest &req, AcceptResponse *res,
                                 rrr::DeferredReply *defer) {
}

}
