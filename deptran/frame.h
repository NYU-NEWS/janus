

#pragma once

namespace rococo {

class Sharding;
class Coordinator;
class TxnChopper;
class TxnRequest;

class Frame {
 public:
  Sharding* CreateSharding();
  Sharding* CreateSharding(Sharding* sd);
  mdb::Row* CreateRow(const mdb::Schema *schema, std::vector<Value> &row_data);
  Coordinator* CreateCoord();
  void GetTxnTypes(std::map<int32_t, std::string> &txn_types);
  TxnChopper* CreateChopper(TxnRequest &req);
  };


} // namespace rococo