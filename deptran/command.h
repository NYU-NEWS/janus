#pragma once
#include "__dep__.h"
#include "constants.h"

namespace rococo {
/**
 * This is the command context to exchange between
 * coordinator and servers, or server to server.
 */
class Command {
public:
  cmdid_t id_ = 0;
  cmdtype_t type_ = 0;
  innid_t inn_id_ = 0;
  cmdid_t root_id_ = 0;
  cmdtype_t root_type_ = 0;

 public:
  virtual innid_t inn_id() const {return inn_id_;}
  virtual cmdtype_t type() {return type_;};
  virtual void Arrest() {verify(0);};
  virtual Command& Execute() {verify(0);};
  virtual void Merge(Command&){verify(0);};;
  virtual bool IsFinished(){verify(0);};

  virtual set<siteid_t> GetPartitionIds() {
    verify(0);
    return set<siteid_t>();
  }
  virtual bool HasMoreSubCmdReadyNotOut() {
    verify(0);
    return false;
  }
  virtual Command* GetNextSubCmd(){verify(0);};
  virtual Command* GetRootCmd() {return this;};
  virtual void Reset() {verify(0);};
  virtual Command* Clone() const  {
    return new Command(*this);
//    verify(0);
  };
};

class SequentialCommand {

};

class ParallelCommand {

};

class ConditionalCommand {

};

class ForCommand {

};


class SimpleCommand: public Command {
 public:
  Command* root_ = nullptr;
  map<int32_t, Value> input = {};
  map<int32_t, Value> output = {};
  int output_size = 0;
  parid_t partition_id_ = 0xFFFFFFFF;
  SimpleCommand() = default;
  virtual parid_t PartitionId() const {
    verify(partition_id_ != 0xFFFFFFFF);
    return partition_id_;
  }
  virtual Command* RootCmd() const {return root_;}
  virtual Command* Clone() const override {
    SimpleCommand* cmd = new SimpleCommand();
    *cmd = *this;
    return cmd;
  }
};


} // namespace rococo
