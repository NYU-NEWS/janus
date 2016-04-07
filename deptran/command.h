#pragma once
#include "__dep__.h"
#include "constants.h"

#define CMD_TPC_PREPARE (1)
#define CMD_TPC_COMMIT  (2)
#define CMD_SIMPLE      (100)
#define CMD_TXN         (200)

namespace rococo {
/**
 * This is the command context to exchange between
 * coordinator and servers, or server to server.
 */
//
//class Command {
//public:
//  cmdid_t id_ = 0;
//  cmdtype_t type_ = 0;
//  innid_t inn_id_ = 0;
//  cmdid_t root_id_ = 0;
//  cmdtype_t root_type_ = 0;
//  Command* cmd = nullptr; // this is the container for real command.
//
// public:
//  virtual innid_t inn_id() const {return inn_id_;}
//  virtual cmdtype_t type() {return type_;};
//  virtual void Arrest() {verify(0);};
//  virtual Command& Execute() {verify(0);};
//  virtual void Merge(Command&){verify(0);};;
//  virtual bool IsFinished(){verify(0);};
//
//  virtual set<siteid_t> GetPartitionIds() {
//    verify(0);
//    return set<siteid_t>();
//  }
//  virtual bool HasMoreSubCmdReadyNotOut() {
//    verify(0);
//    return false;
//  }
//  virtual Command* GetNextReadySubCmd(){verify(0);};
//  virtual Command* GetRootCmd() {return this;};
//  virtual void Reset() {verify(0);};
//  virtual Command* Clone() const  {
//    return new Command(*this);
////    verify(0);
//  };
//  virtual ~Command() {};
//};

class SequentialCommand {

};

class ParallelCommand {

};

class ConditionalCommand {

};

class ForCommand {

};

class ContainerCommand {
 public:
  static map<cmdtype_t, function<ContainerCommand*()>>& Initializers();
  static int RegInitializer(cmdtype_t, function<ContainerCommand*()>);
  static function<ContainerCommand*()> GetInitializer(cmdtype_t);

  cmdid_t id_ = 0;
  cmdtype_t type_ = 0;
  innid_t inn_id_ = 0;
  cmdid_t root_id_ = 0;
  cmdtype_t root_type_ = 0;
  ContainerCommand* self_cmd_ = nullptr;

 public:
  virtual innid_t inn_id() const {return inn_id_;}
  virtual cmdtype_t type() {return type_;};
  virtual void Arrest() {verify(0);};
  virtual ContainerCommand& Execute() {verify(0);};
  virtual void Merge(ContainerCommand&){verify(0);};;
  virtual bool IsFinished(){verify(0);};

  virtual set<siteid_t> GetPartitionIds() {
    verify(0);
    return set<siteid_t>();
  }
  virtual bool HasMoreSubCmdReadyNotOut() {
    verify(0);
    return false;
  }
  virtual ContainerCommand* GetNextReadySubCmd(){verify(0);};
  virtual ContainerCommand* GetRootCmd() {return this;};
  virtual void Reset() {verify(0);};
  virtual ContainerCommand* Clone() const  {
    return new ContainerCommand(*this);
//    verify(0);
  };
  virtual ~ContainerCommand() {};

  virtual Marshal& ToMarshal(Marshal&) const {verify(0);};
  virtual Marshal& FromMarshal(Marshal&) {verify(0);};
};


class SimpleCommand: public ContainerCommand {
 public:
  ContainerCommand* root_ = nullptr;
  map<int32_t, Value> input = {};
  map<int32_t, Value> output = {};
  int32_t output_size = 0;
  parid_t partition_id_ = 0xFFFFFFFF;
  SimpleCommand() = default;
  virtual parid_t PartitionId() const {
    verify(partition_id_ != 0xFFFFFFFF);
    return partition_id_;
  }
  virtual ContainerCommand* RootCmd() const {return root_;}
  virtual ContainerCommand* Clone() const override {
    SimpleCommand* cmd = new SimpleCommand();
    *cmd = *this;
    return cmd;
  }
  virtual ~SimpleCommand() {};
};

} // namespace rococo
