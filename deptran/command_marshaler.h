
#include "__dep__.h"



namespace rococo {

class Command;
class SimpleCommand;
rrr::Marshal &operator<<(rrr::Marshal &m, const Command &cmd);
rrr::Marshal &operator>>(rrr::Marshal &m, Command &cmd);
rrr::Marshal &operator<<(rrr::Marshal &m, const SimpleCommand &cmd);
rrr::Marshal &operator>>(rrr::Marshal &m, SimpleCommand &cmd);

} // namespace rococo