//
// Created by lamont on 1/11/16.
//

#include "MdccDTxn.h"
#include "option.h"
#include "deptran/multi_value.h"

namespace mdcc {
  OptionSet* mdcc::MdccDTxn::CreateUpdateOption(VersionedRow *row, column_id_t col_id, const Value &value) {
    auto locator = std::pair<string, size_t>(row->get_table()->Name(), MultiBlob::hash()(row->get_key()));
    OptionSet* option_set;
    auto it = update_options_.find(locator);
    if (it != update_options_.end()) {
      option_set = it->second;
    } else {
      option_set = new OptionSet(row->get_table()->Name(), row->get_key());
      update_options_.insert(std::pair<std::pair<string, size_t>, OptionSet*>(locator, option_set));
    }
    option_set->Add(Option(col_id, value, row->get_column_ver(col_id)));
    return option_set;
  }

  bool MdccDTxn::WriteColumn(Row *row, column_id_t col_id, const Value &value, int hint_flag) {
    auto versioned_row = dynamic_cast<VersionedRow*>(row);
    assert(versioned_row);
    if (DTxn::WriteColumn(row, col_id, value, hint_flag)) {
      auto option_set = CreateUpdateOption(versioned_row, col_id, value);
      Log_debug("%d updates to key %zu in %s table", option_set->Options().size(), multi_value_hasher()(option_set->Key()), option_set->Table().c_str());
      return true;
    } else {
      return false;
    }
  }
}
