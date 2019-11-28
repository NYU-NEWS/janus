
#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../communicator.h"
#include "../rcc/row.h"
#include "troad.h"

namespace janus {

REG_FRAME(MODE_TROAD, vector<string>({"troad"}), TroadFrame);

} // namespace janus
