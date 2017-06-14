#pragma once

#include "../classic/sched.h"

namespace rococo {

class NoneSched: public ClassicSched {
 using ClassicSched::ClassicSched;
 public:
  virtual int OnDispatch(const vector<SimpleCommand> &v_data,
                         rrr::i32 *res,
                         TxnOutput* output,
                         const function<void()>& callback) override;

  virtual void HandleConflicts(txnid_t txn_id, vector<conf_id_t>& conflicts);

};

}