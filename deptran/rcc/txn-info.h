#pragma once

#include "__dep__.h"
#include "config.h"

namespace rococo {

struct ChopFinishResponse;

class RccGraph;
// TODO Should this class be merged with RCCDTxn?
class TxnInfo {

};
//
//inline rrr::Marshal &operator<<(rrr::Marshal &m, const TxnInfo &ti) {
//  m << ti.txn_id_ << ti.status() << ti.partition_;
//  return m;
//}
//
//inline rrr::Marshal &operator>>(rrr::Marshal &m, TxnInfo &ti) {
//  int8_t status;
//  m >> ti.txn_id_ >> status >> ti.partition_;
//  ti.union_status(status);
//  return m;
//}

} //namespace rcc 
