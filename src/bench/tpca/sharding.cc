#include "sharding.h"
#include "deptran/frame.h"
#include "deptran/scheduler.h"

namespace janus {

int TpcaSharding::PopulateTable(tb_info_t *tb_info_ptr, parid_t par_id) {
//        ---------------------------------------------------------
  mdb::Table *const table_ptr = tx_sched_->get_table(tb_info_ptr->tb_name);
  const mdb::Schema *schema = table_ptr->schema();
//  verify(schema->columns_count() == tb_info_ptr->columns.size());

  Value key_value, max_key;
  mdb::Schema::iterator col_it = schema->begin();
  unsigned int col_index = 0;
  for (; col_index < tb_info_ptr->columns.size(); col_index++) {
    verify(col_it != schema->end());
    verify(tb_info_ptr->columns[col_index].name == col_it->name);
    verify(tb_info_ptr->columns[col_index].type == col_it->type);
    if (tb_info_ptr->columns[col_index].is_primary) {
      verify(col_it->indexed);
      key_value = value_get_zero(tb_info_ptr->columns[col_index].type);
      max_key = value_get_n(tb_info_ptr->columns[col_index].type,
                            tb_info_ptr->num_records);
    }
    col_it++;
  }

  std::vector<Value> row_data;
  for (; key_value < max_key; ++key_value) {
    row_data.clear();
    for (col_index = 0; col_index < tb_info_ptr->columns.size(); col_index++) {
      if (tb_info_ptr->columns[col_index].is_primary) {
        //if (tb_info_ptr->columns[col_index].values != NULL)
        //    tb_info_ptr->columns[col_index].values->push_back(key_value);
        if (par_id != PartitionFromKey(key_value, tb_info_ptr))
          break;
        row_data.push_back(key_value);

        //std::cerr << tb_info_ptr->columns[col_index].name << "(primary):" << row_data.back() << "; ";
      }
      else if (tb_info_ptr->columns[col_index].foreign != NULL) {
//        Value v_buf = value_get_n(tb_info_ptr->columns[col_index].type,
//                                  RandomGenerator::rand(0,
//                                                        tb_info_ptr->columns[col_index].foreign_tb->num_records
//                                                            - 1));
        Value v_buf = value_get_n(tb_info_ptr->columns[col_index].type, 0);
        row_data.push_back(v_buf);

        //std::cerr << tb_info_ptr->columns[col_index].name << "(foreign:" << tb_info_ptr->columns[col_index].foreign->name << "):" << v_buf << "; ";
      }
      else {
        Value v_buf;
        // TODO (ycui) use RandomGenerator
//        v_buf = random_value(tb_info_ptr->columns[col_index].type);
        v_buf.set_i32(0);
        row_data.push_back(v_buf);

        //std::cerr << tb_info_ptr->columns[col_index].name << ":" << v_buf << "; ";
      }
    }
    if (col_index == tb_info_ptr->columns.size()) {
      auto r = frame_->CreateRow(schema, row_data);
      table_ptr->insert(r);
    }
  }
//        ------------------------------------------------------------------

  return 0;
}

bool TpcaSharding::Ready2Populate(tb_info_t *tb_info) {
  return true;
}

} // namespace janus
