//
// Created by lamont on 12/17/15.
//
#include "marshal-value.h"
#include "mdcc.h"

namespace mdcc {

void MdccClientService::Start(const StartRequest& req, StartResponse* res, rrr::DeferredReply* defer) {
  Log_info("in %s", __FUNCTION__);
  res->result = -1;
  defer->reply();
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
