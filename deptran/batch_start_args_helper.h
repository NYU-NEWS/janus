#ifndef BATCH_START_ARGS_HELPER_
#define BATCH_START_ARGS_HELPER_

#include "memdb/value.h"
#include <vector>

namespace rococo {

class BatchStartArgsHelper {
 private:
  uint32_t index_;
  uint32_t piece_count_;
  uint32_t num_piece_;

  typedef enum {
    ARG_INPUT,
    ARG_INPUT_CLIENT,
    ARG_OUTPUT,
    ARG_OUTPUT_CLIENT,
    ARG_NONE
  } arg_type_t;

  arg_type_t arg_type_;

  union {
    std::vector<Value> const *input_;
    std::vector<Value> *output_;
    std::vector<Value> *input_client_;
    std::vector<Value> *output_client_;
  };

 public:
  //BatchStartArgsHelper(size_t size);

  //BatchStartArgsHelper(std::vector<Value> *arg);

  BatchStartArgsHelper();

  int init_input(std::vector<Value> const *input, uint32_t num_piece);

  int get_next_input(i32 *p_type, i64 *pid, Value const **input,
                     i32 *input_size, i32 *output_size);

  int init_output(std::vector<Value> *output, uint32_t num_piece,
                  uint32_t expected_output_size);

  Value *get_output_ptr();

  int put_next_output(i32 res, i32 output_size);

  int done_output();

  int init_input_client(std::vector<Value> *input,
                        uint32_t num_piece, uint32_t input_size);

  int put_next_input_client(std::vector<Value> &input,
                            i32 p_type, i64 pid, i32 output_size);

  int done_input_client();

  int init_output_client(std::vector<Value> *output,
                         uint32_t num_piece);

  int get_next_output_client(i32 *res, Value const **output,
                             uint32_t *output_size);

};

}

#endif
