#include "deptran/__dep__.h"
#include "procedure.h"

namespace janus {

TpccDistChopper::TpccDistChopper() {
}

void TpccDistChopper::new_order_shard(const char *tb,
                                      const vector<Value> &input,
                                      uint32_t &site,
                                      int cnt) {
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT
      || tb == TPCC_TB_CUSTOMER
      || tb == TPCC_TB_ORDER
      || tb == TPCC_TB_NEW_ORDER
      || tb == TPCC_TB_ORDER_LINE)
    // based on d_id && w_id
    mv = MultiValue(std::vector<Value>({input[1], input[0]}));
  else if (tb == TPCC_TB_WAREHOUSE)
    // based on w_id
    mv = MultiValue(input[0]);
  else if (tb == TPCC_TB_ITEM)
    // based on i_id
    mv = MultiValue(input[4 + 3 * cnt]);
  else if (tb == TPCC_TB_STOCK)
    // based on s_i_id && s_w_id
    mv = MultiValue(std::vector<Value>({input[4 + 3 * cnt],
                                        input[5 + 3 * cnt]}));
  else
    verify(0);
  int ret = sss_->GetPartition(tb, mv, site);
  verify(ret == 0);
}

void TpccDistChopper::payment_shard(const char *tb,
                                    const vector<Value> &input,
                                    uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_WAREHOUSE)
    // based on w_id
    mv = MultiValue(input[0]);
  else if (tb == TPCC_TB_DISTRICT)
    // based on d_id && w_id
    mv = MultiValue(std::vector<Value>({input[1], input[0]}));
  else if (tb == TPCC_TB_CUSTOMER)
    // based on c_d_id && c_w_id
    mv = MultiValue(std::vector<Value>({input[4], input[3]}));
  else if (tb == TPCC_TB_HISTORY)
    // based on h_key
    mv = MultiValue(input[6]);
  else
    verify(0);
  int ret = sss_->GetPartition(tb, mv, site);
  verify(ret == 0);
}

void TpccDistChopper::order_status_shard(const char *tb,
                                         const vector<mdb::Value> &input,
                                         uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_CUSTOMER
      || tb == TPCC_TB_ORDER
      || tb == TPCC_TB_ORDER_LINE)
    // based on d_id && w_id
    mv = MultiValue(std::vector<Value>({input[1], input[0]}));
  else
    verify(0);
  int ret = sss_->GetPartition(tb, mv, site);
  verify(ret == 0);
}

void TpccDistChopper::delivery_shard(const char *tb,
                                     const vector<mdb::Value> &input,
                                     uint32_t &site,
                                     int cnt) {
  MultiValue mv;
  if (tb == TPCC_TB_NEW_ORDER
      || tb == TPCC_TB_ORDER
      || tb == TPCC_TB_ORDER_LINE
      || tb == TPCC_TB_CUSTOMER)
    // based on d_id && w_id
    mv = MultiValue(std::vector<Value>({Value((i32) cnt), input[0]}));
  else
    verify(0);
  int ret = sss_->GetPartition(tb, mv, site);
  verify(ret == 0);
}

void TpccDistChopper::stock_level_shard(const char *tb,
                                        const vector<mdb::Value> &input,
                                        uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT
      || tb == TPCC_TB_ORDER_LINE)
    // based on d_id & w_id
    mv = MultiValue(std::vector<Value>({input[1], input[0]}));
  else if (tb == TPCC_TB_STOCK)
    // based on s_i_id & s_w_id
    mv = MultiValue(std::vector<Value>({input[0], input[1]}));
  else
    verify(0);
  int ret = sss_->GetPartition(tb, mv, site);
  verify(ret == 0);
}

TpccDistChopper::~TpccDistChopper() {
}

}
