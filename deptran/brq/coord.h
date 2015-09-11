#pragma once

#include "all.h"
#define phase_t uint64_t

namespace rococo {

class BRQCoordinator: public Coordinator {
public:
  rrr::PollMgr *rpc_poll_;

  // TODO should be replaced with Commo
  // for message passing
  std::vector<rrr::Client*> vec_rpc_cli_;
  std::vector<RococoProxy *> vec_rpc_proxy_;
  ClientControlServiceImpl *ccsi_;

  cooid_t coo_id_;
  phase_t phase_; // a phase identifier
  int bench_;
  uint32_t thread_id_;

  Mutex mtx_;

  std::vector<int> site_prepare_;
  std::vector<int> site_commit_;
  std::vector<int> site_abort_;
  std::vector<int> site_piece_;
  Recorder *recorder_;

  Command cmd_;

  BRQCoordinator(uint32_t coo_id,
              std::vector<std::string>& addrs,
              int benchmark,
              ClientControlServiceImpl *ccsi = NULL,
              uint32_t thread_id = 0,
              bool batch_optimal = false);

  virtual ~BRQCoordinator() {
  }

  void restart();
  void launch(Command&);
  void fast_accept();
  void prepare();
  void accept();
  void commit();
  void reset(); // reuse for next cmd.

}
}// namespace rococo
