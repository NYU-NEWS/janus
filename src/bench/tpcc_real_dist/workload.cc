
#include "workload.h"

namespace janus {

TpccRdWorkload::TpccRdWorkload(Config* config)
    : TpccWorkload(config) {
  std::map<std::string, uint64_t> table_num_rows;
  sharding_->get_number_rows(table_num_rows);

  std::vector<unsigned int> partitions;
  sharding_->GetTablePartitions(TPCC_TB_WAREHOUSE, partitions);
  uint64_t tb_w_rows = table_num_rows[std::string(TPCC_TB_WAREHOUSE)];
  tpcc_para_.n_w_id_ = (int) tb_w_rows;
  tpcc_para_.const_home_w_id_ = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
  uint64_t tb_d_rows = table_num_rows[std::string(TPCC_TB_DISTRICT)];
  tpcc_para_.n_d_id_ = (int) tb_d_rows * partitions.size() / tb_w_rows;
  uint64_t tb_c_rows = table_num_rows[std::string(TPCC_TB_CUSTOMER)];
  tpcc_para_.n_c_id_ = (int) tb_c_rows / tb_d_rows;
  tpcc_para_.n_i_id_ = (int) table_num_rows[std::string(TPCC_TB_ITEM)];
  tpcc_para_.delivery_d_id_ = RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1);
  switch (single_server_) {
    case Config::SS_DISABLED:
      fix_id_ = -1;
      break;
    case Config::SS_THREAD_SINGLE:
    case Config::SS_PROCESS_SINGLE: {
      fix_id_ = Config::GetConfig()->get_client_id() % tpcc_para_.n_d_id_;
      tpcc_para_.const_home_w_id_ =
          Config::GetConfig()->get_client_id() / tpcc_para_.n_d_id_;
      break;
    }
    default:
      verify(0);
  }
}


void TpccRdWorkload::RegOrderStatus() {
  TpccWorkload::RegOrderStatus();
  SHARD_PIE(TPCC_ORDER_STATUS, TPCC_ORDER_STATUS_0, TPCC_TB_CUSTOMER,
            TPCC_VAR_D_ID, TPCC_VAR_W_ID);
  SHARD_PIE(TPCC_ORDER_STATUS, TPCC_ORDER_STATUS_1, TPCC_TB_CUSTOMER,
            TPCC_VAR_D_ID, TPCC_VAR_W_ID)
  SHARD_PIE(TPCC_ORDER_STATUS, TPCC_ORDER_STATUS_2, TPCC_TB_ORDER,
            TPCC_VAR_D_ID, TPCC_VAR_W_ID)
  SHARD_PIE(TPCC_ORDER_STATUS, TPCC_ORDER_STATUS_3, TPCC_TB_ORDER_LINE,
            TPCC_VAR_D_ID, TPCC_VAR_W_ID)
}

void TpccRdWorkload::RegStockLevel() {
  TpccWorkload::RegStockLevel();
  SHARD_PIE(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_0, TPCC_TB_DISTRICT,
            TPCC_VAR_D_ID, TPCC_VAR_W_ID)
  SHARD_PIE(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_1, TPCC_TB_ORDER_LINE,
            TPCC_VAR_D_ID, TPCC_VAR_W_ID)
  for (int i = (0); i < (1000); i++) {
    SHARD_PIE(TPCC_STOCK_LEVEL, TPCC_STOCK_LEVEL_RS(i), TPCC_TB_STOCK,
              TPCC_VAR_OL_I_ID(i), TPCC_VAR_W_ID)
  }
}

} // namespace janus
