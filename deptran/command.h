#pragma once

namespace rococo {
/**
 * This is the command context to exchange between
 * coordinator and servers, or server to server.
 */
class Command {
public:
  virtual std::vector<groupid_t>& GetGroups();
  virtual void Arrest();
  virtual Command& Execute();
  virtual void Merge(Command&);
  virtual bool IsFinished();
  virtual Command* GetNextSubCmd(std::map<uint64_t, Command*>);
  virtual Command* HasMoreSubCmd(std::map<uint64_t, Command*>);
  // virtual void HasMoreContext();
};


class LegacyCommand : public Command {

};

} // namespace rococo
