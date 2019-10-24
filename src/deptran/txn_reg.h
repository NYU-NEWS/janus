#pragma once
#include "__dep__.h"
#include "constants.h"
#include "command.h"
namespace janus {

#define TXN_GENERAL  (0x0)
#define TXN_ONE_SHOT (0x1)

class RequestHeader;
class Executor;

typedef std::map<
    int,
    mdb::Row *> row_map_t;

class Tx;
class TxData;
class TxRequest;
class SimpleCommand;

typedef std::function<void(Executor* exec,
                           Tx& tx,
                           SimpleCommand& cmd,
                           rrr::i32 *res,
                           map<int32_t, Value> &output
)> ProcHandler;

typedef enum {
} defer_t;

typedef std::function<bool(TxData *,
                           map<int32_t, Value>&)>
    PieceCallbackHandler;

struct conf_id_t {
  string table{};
  std::vector<int> primary_keys{};
  std::vector<int> columns{};
  int row_context_id{};

  conf_id_t(string t,
          vector<int> k,
          vector<int> c,
          int rc_id) : table(t),
                       primary_keys(k),
                       columns(c),
                       row_context_id(rc_id) {
  }
};

typedef std::pair<string, vector<int32_t> > sharder_t;

class TxnPieceDef {
 public:
  ProcHandler proc_handler_{};
  PieceCallbackHandler callback_{};
  sharder_t sharder_{};
  rank_t rank_{};
  set<int32_t> input_vars_{};
  set<int32_t> output_vars_{};
  vector<conf_id_t> conflicts_{};
  vector<string> conflicts_str() {
    // TODO
    return {};
  };
};

/**
* This class holds all the hard-coded transactions pieces.
*/
class TxnRegistry {
 public:

  TxnRegistry() {};

  /*
   * return the piece definition, so maybe change a name?
   */
  TxnPieceDef& get(const txntype_t t_type, const innid_t p_type) {
    return regs_[t_type][p_type];
  }

 public:
  // prevent instance creation
  // TxnRegistry() { }
  map<txntype_t, int> txn_types_{};
  map<txntype_t, map<innid_t, TxnPieceDef>> regs_{};
};

} // namespace janus
