//
// Created by lamont on 12/22/15.
//

#include "communicator.h"

namespace mdcc {
void MdccCommunicator::SendStart(groupid_t gid,
                                 RequestHeader &header,
                                 map <int32_t, Value> &input,
                                 int32_t output_size,
                                 std::function<void(Future * fu)> &callback) { }

void MdccCommunicator::SendStart(parid_t par_id,
                                 rococo::StartRequest &req,
                                 Coordinator *coo,
                                 std::function<void(rococo::StartReply & )> &callback) { }

void MdccCommunicator::SendPrepare(parid_t gid,
                                   txnid_t tid,
                                   std::vector <int32_t> &sids,
                                   std::function<void(Future * fu)> &callback) { }

void MdccCommunicator::SendCommit(parid_t pid, txnid_t tid,
                                  std::function<void(Future * fu)> &callback) { }

void MdccCommunicator::SendAbort(parid_t pid, txnid_t tid,
                                 std::function<void(Future * fu)> &callback) { }
}
