#pragma once
#include "__dep__.h"
#include "constants.h"

#define CMD_TPC_PREPARE (1)
#define CMD_TPC_COMMIT  (2)
#define CMD_SIMPLE      (100)
#define CMD_TXN         (200)

namespace rococo {
/**
 * TODO: maybe re-write this into a marshalling proxy?
 * Reason why need such a helper: it is possible to marshall an object
 * without knowing its actual type; but it is difficult to unmarshal from
 * stream without knowing its actual type (only know the base type).
 */
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
  ContainerCommand* self_cmd_ = nullptr; // point to actual stuff.

 public:
  virtual innid_t inn_id() const {return inn_id_;}
  virtual cmdtype_t type() {return type_;};
  virtual void Arrest() {verify(0);};
  virtual ContainerCommand& Execute() {verify(0);};
  virtual void Merge(ContainerCommand&){verify(0);};;
  virtual bool IsFinished(){verify(0);};

  virtual set<parid_t> GetPartitionIds() {
    verify(0);
    return set<parid_t>();
  }

  virtual bool HasMoreSubCmdReadyNotOut() {
    verify(0);
    return false;
  }

  virtual ContainerCommand* GetNextReadySubCmd(){verify(0);};
  virtual ContainerCommand* GetRootCmd() {return this;};
  virtual void Reset() {verify(0);};
  virtual ContainerCommand* Clone() const  {
    ContainerCommand* c = GetInitializer(type_)();
    *c = *this;
    c->self_cmd_ = c;
    return c;
  };

  virtual ~ContainerCommand() {};
  virtual Marshal& ToMarshal(Marshal&) const {verify(0);};
  virtual Marshal& FromMarshal(Marshal&) {verify(0);};
};
} // namespace rococo
