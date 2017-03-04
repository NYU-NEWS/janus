#pragma once
#include "__dep__.h"
#include "constants.h"
#include "command.h"
namespace rococo {

#define TXN_GENERAL  (0x0)
#define TXN_ONE_SHOT (0x1)

class RequestHeader;
class Executor;

typedef std::map<
    int,
    mdb::Row *> row_map_t;

class DTxn;
class Procedure;
class TxnRequest;
class SimpleCommand;

typedef std::function<void(Executor* exec,
                           DTxn *dtxn,
                           SimpleCommand& cmd,
                           rrr::i32 *res,
                           map<int32_t, Value> &output
)> ProcHandler;

typedef enum {
  DF_REAL,
  DF_NO,
  DF_FAKE
} defer_t;

typedef struct {
  ProcHandler txn_handler;
  defer_t defer;
} ProcHandlerPair;

typedef std::function<bool(Procedure *,
                           map<int32_t, Value>&)>
    PieceCallbackHandler;


struct conf_id {
  string table{};
  std::vector<int> primary_keys{};
  std::vector<int> columns{};
  int row_context_id{};

  conf_id(string t,
          vector<int> k,
          vector<int> c,
          int rc_id) : table(t),
                       primary_keys(k),
                       columns(c),
                       row_context_id(rc_id) {
  }
};

class LeafProcedure {
 public:
  ProcHandlerPair proc_handler_pair_{};
  PieceCallbackHandler callback_{};
  pair<string, vector<int32_t>> sharding_input_{};
  set<int32_t> input_vars_{};
  set<int32_t> output_vars_{};
  vector<conf_id> conflicts_{};
};

/**
* This class holds all the hard-coded transactions pieces.
*/
class TxnRegistry {
 public:

  TxnRegistry() {};

  void reg(txntype_t t_type,
           innid_t p_type,
           defer_t defer,
           const ProcHandler &txn_handler) {
    regs_[t_type][p_type].proc_handler_pair_ =
        (ProcHandlerPair) {txn_handler, defer};
  }

  ProcHandlerPair& get(const txntype_t t_type,
                       const innid_t p_type) {
    auto& pair = regs_[t_type][p_type].proc_handler_pair_;
    verify(pair.txn_handler);
    return pair;
  }
  ProcHandlerPair& get(const SimpleCommand &);

 public:
  // prevent instance creation
  // TxnRegistry() { }
  map<txntype_t, int> txn_types_{};
//  map<std::pair<base::i32, base::i32>, ProcHandlerPair> all_{};
//  map<std::pair<txntype_t, innid_t>, PieceCallbackHandler> callbacks_{};
//  map<std::pair<txntype_t, innid_t>,
//      std::pair<string, vector<int32_t>>> sharding_input_{};
//  map<txntype_t, map<innid_t, set<int32_t>>> input_vars_{};
//  map<txntype_t, map<innid_t, set<int32_t>>> output_vars_{};
//  map<txntype_t, std::function<void(Procedure * ch, TxnRequest& req)> > init_{};
//  map<txntype_t, std::function<void(Procedure * ch)>> retry_{};
//  map<txntype_t, map<innid_t, vector<conf_id>>> conflicts_{};
  map<txntype_t, map<innid_t, LeafProcedure>> regs_{};
};

} // namespace rococo