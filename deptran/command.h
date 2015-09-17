#pragma once

namespace rococo {
class CommandOutput {
  // TODO
};

class Command {
  // TODO
public:
  virtual std::vector<groupid_t>& get_groups();
  virtual CommandOutput pre_execute();
  virtual CommandOutput execute();
  virtual void feed(CommandOutput &output);
  virtual bool is_empty();
};

/**
 * consists many simple command.
 */
class NestedCommand: public Command {

};
} // namespace rococo
