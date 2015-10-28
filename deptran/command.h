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
  cmdtype_t type_;
  innid_t inn_id_;
public:
  virtual innid_t inn_id() {return inn_id_;}
  virtual cmdtype_t type() {return type_;};
  virtual parid_t GetPar() {verify(0);};
  virtual void Arrest() {verify(0);};
  virtual Command& Execute() {verify(0);};
  virtual void Merge(Command&){verify(0);};;
  virtual bool IsFinished(){verify(0);};

  virtual set<parid_t> GetPars() {verify(0); return set<parid_t>();};
  virtual bool IsSinglePar(){verify(0); return false;};
  virtual bool HasMoreSubCmd(){verify(0); return false;};
  virtual Command* GetNextSubCmd(){verify(0);};
  virtual Command* GetRootCmd() {return this;};
  virtual void Reset() {verify(0);};
};

class SequentialCommand {

};

class ParallelCommand {

};

class ConditionalCommand {

};

class ForCommand {

};

} // namespace rococo
