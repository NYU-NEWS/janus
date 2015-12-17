#pragma once
#include "__dep__.h"
namespace rococo {

class RequestHeader;
class Executor;

typedef std::map<
    int,
    mdb::Row *> row_map_t;

class DTxn;

typedef std::function<void(
    Executor* exec,
    DTxn *dtxn,
    const RequestHeader &header,
    map<int32_t, Value> &input,
    rrr::i32 *res,
    map<int32_t, Value> &output,
    rrr::i32 *output_size//,
//    row_map_t *row_map
)> TxnHandler;

typedef enum {
  DF_REAL,
  DF_NO,
  DF_FAKE
} defer_t;

typedef struct {
  TxnHandler txn_handler;
  defer_t defer;
} txn_handler_defer_pair_t;

/**
* This class holds all the hard-coded transactions pieces.
*/
class TxnRegistry {
 public:

  void reg(base::i32 t_type, base::i32 p_type,
           defer_t defer, const TxnHandler &txn_handler) {
    auto func_key = std::make_pair(t_type, p_type);
    auto it = all_.find(func_key);
    verify(it == all_.end());
    all_[func_key] = (txn_handler_defer_pair_t) {txn_handler, defer};
  }

  txn_handler_defer_pair_t get(const base::i32 t_type,
                               const base::i32 p_type) {
    auto it = all_.find(std::make_pair(t_type, p_type));
    // Log::debug("t_type: %d, p_type: %d", t_type, p_type);
    verify(it != all_.end());
    return it->second;
  }
  txn_handler_defer_pair_t get(const RequestHeader &req_hdr);

 private:
  // prevent instance creation
//  TxnRegistry() { }
  map<std::pair<base::i32, base::i32>, txn_handler_defer_pair_t> all_;
//    static map<std::pair<base::i32, base::i32>, LockSetOracle> lck_oracle_;

};


} // namespace rococo