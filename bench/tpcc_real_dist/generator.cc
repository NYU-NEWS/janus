

#include "generator.h"
#include "piece.h"

using namespace rococo;

TpccdTxnGenerator::TpccdTxnGenerator(Config* config)
    : TpccTxnGenerator(config) {
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

