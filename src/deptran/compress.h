# pragma once

#include "__dep__.h"

namespace janus {

class Compressor {
 public:
  static void vector_to_string(
      const std::vector<int64_t> &id,
      std::string *out
  ) {
    const char *data = (const char *) id.data();
    //    snappy::Compress(data, id.size() * sizeof(int64_t), out);
    *out = std::string(data, id.size() * sizeof(int64_t));
  }

  static void string_to_vector(
      const std::string &in,
      std::vector<int64_t> *ids
  ) {
    std::string output;
    //    snappy::Uncompress(in.data(), in.size(), &output);
    output = in;
    int64_t *tmp = (int64_t *) output.data();
    int n = output.size() / sizeof(int64_t);
    for (int i = 0; i < n; i++) {
      ids->push_back(*(tmp + i));
    }
  }
};

} // namespace janus
