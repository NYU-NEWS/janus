#pragma once

#include <string.h>
#include "threading.hpp"

//#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define __FILENAME__ __FILE__

#define Log_debug(msg, ...) ::rrr::Log::debug(__LINE__, __FILENAME__, msg, ## __VA_ARGS__)
#define Log_info(msg, ...) ::rrr::Log::info(__LINE__, __FILENAME__, msg, ## __VA_ARGS__)
#define Log_warn(msg, ...) ::rrr::Log::warn(__LINE__, __FILENAME__, msg, ## __VA_ARGS__)
#define Log_error(msg, ...) ::rrr::Log::error(__LINE__, __FILENAME__, msg, ## __VA_ARGS__)
#define Log_fatal(msg, ...) ::rrr::Log::fatal(__LINE__, __FILENAME__, msg, ## __VA_ARGS__)

namespace rrr {

class Log {
    static int level_s;
    static FILE* fp_s;

    // have to use pthread mutex because Mutex class cannot be init'ed correctly as static var
    static pthread_mutex_t m_s;

    static void log_v(int level, int line, const char* file, const char* fmt, va_list args);
public:

    enum {
        FATAL = 0, ERROR = 1, WARN = 2, INFO = 3, DEBUG = 4
    };

    static void set_file(FILE* fp);
    static void set_level(int level);

    static void log(int level, int line, const char* file, const char* fmt, ...);

    static void fatal(int line, const char* file, const char* fmt, ...);
    static void error(int line, const char* file, const char* fmt, ...);
    static void warn(int line, const char* file, const char* fmt, ...);
    static void info(int line, const char* file, const char* fmt, ...);
    static void debug(int line, const char* file, const char* fmt, ...);

    static void fatal(const char* fmt, ...);
    static void error(const char* fmt, ...);
    static void warn(const char* fmt, ...);
    static void info(const char* fmt, ...);
    static void debug(const char* fmt, ...);
};

} // namespace base
