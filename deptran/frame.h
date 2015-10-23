

#pragma once

namespace rococo {

class Sharding;
class Coordinator;

class Frame {
 public:
  Sharding* CreateSharding();
  mdb::Row* CreateRow(const mdb::Schema *schema, std::vector<Value> &row_data);
  Coordinator* CreateCoord();
};


} // namespace rococo