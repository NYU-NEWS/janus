#pragma once

#include "command.h"
#include "dep_graph.h"

namespace rococo {
//
//struct FastAcceptRequest {
//  cmdid_t  cmd_id; // used as instance locater
//  cooid_t  coo_id;
//  ballot_t ballot;
//  ContainerCommand  cmd;
//};
//
//struct FastAcceptReply {
//  bool     ack; // true or false: yes or no
//  BrqGraph deps;
//};
//
//struct PrepareReqeust {
//  cmdid_t  cmd_id;
//  cooid_t  coo_id;
//  ballot_t ballot;
//};
//
//struct PrepareReply {
//  bool     ack;
//  ballot_t ballot_cmd_vote;
//  ballot_t ballot_deps_vote;
//  CotnainerCommand  cmd;  // optional
//  BrqGraph deps; // optional
//};
//
//struct AcceptRequest {
//  cmdid_t cmd_id;
//  ballot_t ballot;
//  Command  cmd;
//  BrqGraph deps;
//};
//
//struct AcceptReply {
//  bool ack;
//};
//
//struct CommitRequest {
//  cmdid_t cmd_id;
//  ballot_t ballot;
//  Command  cmd;
//  BrqGraph subgraph;
//};
//
//struct CommitReply {
//  Command output;
//};
//
//struct InquiryReply {
//
//};

// struct InquireRequest {
//   cmdid_t cmd_id;
// }
//
// struct InquireReply {
//   SubGraph deps;
// }
//
} // namespace rococo
