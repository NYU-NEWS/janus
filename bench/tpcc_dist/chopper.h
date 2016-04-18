#ifndef TPCC_DIST_CHOPPER_H_
#define TPCC_DIST_CHOPPER_H_

#include "../tpcc/chopper.h"

namespace rococo {

class TpccTxn;

class TpccDistChopper: public TpccTxn {
 protected:
  virtual void new_order_shard(const char *tb,
                               const std::vector<mdb::Value> &input,
                               uint32_t &site,
                               int cnt = 0);

  virtual void payment_shard(const char *tb,
                             const vector<mdb::Value> &input,
                             uint32_t &site);

  virtual void order_status_shard(const char *tb,
                                  const vector<mdb::Value> &input,
                                  uint32_t &site);

  virtual void delivery_shard(const char *tb,
                              const vector<mdb::Value> &input,
                              uint32_t &site,
                              int cnt);

  virtual void stock_level_shard(const char *tb,
                                 const vector<mdb::Value> &input,
                                 uint32_t &site);

 public:
  TpccDistChopper();

  virtual ~TpccDistChopper();
};

} // namespace rococo

#endif
