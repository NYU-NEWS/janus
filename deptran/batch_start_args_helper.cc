#include "__dep__.h"
#include "batch_start_args_helper.h"

namespace rococo {

BatchStartArgsHelper::BatchStartArgsHelper() {
  arg_type_ = ARG_NONE;
}

int BatchStartArgsHelper::init_input(std::vector<Value> const *input, uint32_t num_piece) {
  if (arg_type_ != ARG_NONE)
    return -1;
  arg_type_ = ARG_INPUT;
  index_ = 0;
  piece_count_ = 0;
  num_piece_ = num_piece;
  input_ = input;
  return 0;
}

int BatchStartArgsHelper::get_next_input(i32 *p_type, i64 *pid,
                                         Value const **input, i32 *input_size, i32 *output_size) {

  verify(arg_type_ == ARG_INPUT);
  if (piece_count_ >= num_piece_)
    return -1;
  *p_type = (*input_)[index_++].get_i32();
  *pid = (*input_)[index_++].get_i64();
  *input_size = (*input_)[index_++].get_i32();
  *output_size = (*input_)[index_++].get_i32();
  *input = input_->data() + index_;
  index_ += *input_size;
  piece_count_++;

  return 0;
}

int BatchStartArgsHelper::init_output(std::vector<Value> *output,
                                      uint32_t num_piece, uint32_t expected_output_size) {

  if (arg_type_ != ARG_NONE)
    return -1;
  arg_type_ = ARG_OUTPUT;
  index_ = 0;
  piece_count_ = 0;
  num_piece_ = num_piece;
  output_ = output;
  output_->resize(num_piece * 2 + expected_output_size);

  return 0;
}

Value *BatchStartArgsHelper::get_output_ptr() {

  verify(arg_type_ == ARG_OUTPUT);
  return output_->data() + index_ + 2;
}

int BatchStartArgsHelper::put_next_output(i32 res, i32 output_size) {

  verify(arg_type_ == ARG_OUTPUT);
  if (piece_count_ >= num_piece_)
    return -1;
  (*output_)[index_++] = Value(res);
  (*output_)[index_++] = Value(output_size);
  index_ += output_size;
  piece_count_++;

  return 0;
}

int BatchStartArgsHelper::done_output() {

  verify(arg_type_ == ARG_OUTPUT);
  if (piece_count_ < num_piece_)
    return -1;
  output_->resize(index_);

  return 0;
}

int BatchStartArgsHelper::init_input_client(std::vector<Value> *input,
                                            uint32_t num_piece, uint32_t input_size) {

  if (arg_type_ != ARG_NONE)
    return -1;
  arg_type_ = ARG_INPUT_CLIENT;
  index_ = 0;
  piece_count_ = 0;
  num_piece_ = num_piece;
  input_client_ = input;
  input_client_->resize(num_piece * 4 + input_size);

  return 0;
}

int BatchStartArgsHelper::put_next_input_client(std::vector<Value> &input,
                                                i32 p_type, i64 pid, i32 output_size) {

  verify(arg_type_ = ARG_INPUT_CLIENT);
  if (piece_count_ >= num_piece_)
    return -1;
  (*input_client_)[index_++] = Value(p_type);
  (*input_client_)[index_++] = Value(pid);
  (*input_client_)[index_++] = Value((i32) input.size());
  (*input_client_)[index_++] = Value(output_size);
  size_t i = 0;
  for (; i < input.size(); i++)
    (*input_client_)[index_++] = input[i];
  piece_count_++;
  return 0;
}

int BatchStartArgsHelper::done_input_client() {

  verify(arg_type_ == ARG_INPUT_CLIENT);
  if (piece_count_ == num_piece_ && index_ == input_client_->size())
    return 0;
  return -1;
}

int BatchStartArgsHelper::init_output_client(std::vector<Value> *output, uint32_t num_piece) {

  if (arg_type_ != ARG_NONE)
    return -1;
  arg_type_ = ARG_OUTPUT_CLIENT;
  index_ = 0;
  piece_count_ = 0;
  num_piece_ = num_piece;
  output_client_ = output;

  return 0;
}

int BatchStartArgsHelper::get_next_output_client(i32 *res,
                                                 Value const **output, uint32_t *output_size) {

  verify(arg_type_ == ARG_OUTPUT_CLIENT);
  if (piece_count_ >= num_piece_)
    return -1;
  *res = (*output_client_)[index_++].get_i32();
  i32 output_size_buf;
  output_size_buf = (*output_client_)[index_++].get_i32();
  *output_size = (uint32_t) output_size_buf;
  *output = output_client_->data() + index_;
  index_ += output_size_buf;
  piece_count_++;

  return 0;
}

}
