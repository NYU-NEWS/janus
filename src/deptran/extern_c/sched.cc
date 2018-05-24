//
// Created by Shuai on 6/27/17.
//

#include "sched.h"

// disable for now.

//extern "C" {
//
//#include "../../../extern_interface/scheduler.h"
//
//}
//
//namespace janus {
//
//bool ExternCScheduler::HandleConflicts(Tx &dtxn,
//                                       innid_t inn_id,
//                                       vector<string> &conflicts) {
//  // TODO call C interface
//  vector<_c_conflict_id_t> cf(conflicts.size());
//  for (int i = 0; i < conflicts.size(); i++) {
//    cf[i].key = const_cast<char*>(conflicts[i].data());
//    cf[i].ptr_mem = nullptr;
//    cf[i].rw_flag = 1;
//    cf[i].sz_key = conflicts[i].size();
//  }
//    int r = ::handle_conflicts(dtxn.tid_, conflicts.size(), cf.data());
//    return (r > 0);
//  return true;
//}
//
//} // namespace janus
