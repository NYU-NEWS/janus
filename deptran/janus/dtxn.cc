
#include "dtxn.h"

namespace rococo {

void JanusDTxn::DispatchExecute(SimpleCommand &cmd,
                              int32_t *res,
                              map<int32_t, Value> *output) {
  for (auto& c: dreqs_) {
    if (c.inn_id() == cmd.inn_id()) // already handled?
      return;
  }
  verify(txn_reg_);
  auto pair = txn_reg_->get(cmd);
  auto& conflicts = txn_reg_->regs_[cmd.root_type_][cmd.type()].conflicts_;
  for (auto& c: conflicts) {
    vector<Value> pkeys;
    for (int i = 0; i < c.primary_keys.size(); i++) {
      pkeys.push_back(cmd.input.at(c.primary_keys[i]));
    }
    auto row = Query(GetTable(c.table), pkeys, c.row_context_id);
    verify(row != nullptr);
    for (auto col_id : c.columns) {
      TraceDep(row, col_id, TXN_DEFERRED);
    }
  }
  dreqs_.push_back(cmd);
}

} // namespace rococo
