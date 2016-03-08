

#include "sched.h"
#include "dtxn.h"

namespace rococo {

int RccSched::OnHandoutRequest(const SimpleCommand &cmd,
                               int32_t *res,
                               map<int32_t, Value> *output,
                               RccGraph *graph,
                               const function<void()> &callback) {
  RccDTxn *dtxn = (RccDTxn *) GetOrCreateDTxn(cmd.root_id_);

  auto job = [&cmd, res, dtxn, callback, graph, output] () {
    dtxn->StartAfterLog(cmd, res, output);
    dep_graph_->sub_txn_graph(cmd.id_, graph);
    callback();
  };

  static bool do_record = Config::GetConfig()->do_logging();
  if (do_record) {
    Marshal m;
    m << cmd;
    recorder_->submit(m, job);
  } else {
    job();
  }
}

} // namespace rococo