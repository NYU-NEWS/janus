//
// Created by lamont on 12/17/15.
//
#include "mdcc.h"

namespace mdcc {
  void MdccClientService::Start(const StartRequest &req, StartResponse *res) {
  }

  void MdccLearnerService::Learn(const Result& r) {
  }

  void MdccLeaderService::Propose(const ProposeRequest &req, ProposeResponse *res, rrr::DeferredReply *defer) {
  }

  void MdccLeaderService::Recover() {
  }

  void MdccAcceptorService::Propose(const ProposeRequest &req, ProposeResponse *res, rrr::DeferredReply *defer) {

  }

  void mdcc::MdccAcceptorService::ProposeFast(const ProposeRequest &req, ProposeResponse *res, rrr::DeferredReply *defer) {

  }

  void MdccAcceptorService::Decide(const Result &result, rrr::DeferredReply *defer) {
  }

  void MdccAcceptorService::Accept(const AcceptRequest &req, AcceptResponse *res, rrr::DeferredReply *defer) {
  }
}