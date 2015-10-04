#pragma once
#include "__dep__.h"
#include "constants.h"

namespace rococo {
/**
 * This is the command context to exchange between
 * coordinator and servers, or server to server.
 */
class Command {
private:
  cmdtype_t type_;
  innid_t inn_id_;
public:
  virtual innid_t inn_id() {return inn_id_;}
  virtual cmdtype_t type() {return type_;};
  virtual parid_t GetPar() {verify(0);};
  virtual std::vector<parid_t>& GetPars() {verify(0);};
  virtual void Arrest() {verify(0);};
  virtual Command& Execute() {verify(0);};
  virtual void Merge(Command&){verify(0);};;
  virtual bool IsFinished(){verify(0);};
  virtual bool HasMoreSubCmd(map<innid_t, Command*>&){verify(0);};;
  virtual Command* GetNextSubCmd(std::map<innid_t, Command*>&){verify(0);};
  // virtual void HasMoreContext();
};


class LegacyCommand : public Command {

};

} // namespace rococo
