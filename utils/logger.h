/*
 * logger.h
 *
 *  Created on: Nov 9, 2012
 *      Author: frog
 */


#ifndef LOGGER_H_
#define LOGGER_H_

#include <stdio.h>
#include <apr_time.h>

#define LOG_LEVEL_OFF    0
#define LOG_LEVEL_FATAL  1
#define LOG_LEVEL_ERROR  2
#define LOG_LEVEL_WARN   3
#define LOG_LEVEL_INFO   4
#define LOG_LEVEL_DEBUG  5
#define LOG_LEVEL_TRACE  6
#define LOG_LEVEL_ALL    6

#ifndef LOG_LEVEL
#ifdef NDEBUG
#define LOG_LEVEL LOG_LEVEL_INFO
#else
#define LOG_LEVEL LOG_LEVEL_DEBUG
#endif // NDEBUG
#endif // LOG_LEVEL

/* Change this to change log level */
//#define LOG_LEVEL        LOG_LEVEL_TRACE
//#define LOG_LEVEL        LOG_LEVEL_DEBUG
//#define LOG_LEVEL        LOG_LEVEL_INFO


#define LOG_FATAL_ENABLED (LOG_LEVEL >= LOG_LEVEL_FATAL)
#define LOG_ERROR_ENABLED (LOG_LEVEL >= LOG_LEVEL_ERROR)
#define LOG_WARN_ENABLED  (LOG_LEVEL >= LOG_LEVEL_WARN)
#define LOG_INFO_ENABLED  (LOG_LEVEL >= LOG_LEVEL_INFO)
#define LOG_DEBUG_ENABLED (LOG_LEVEL >= LOG_LEVEL_DEBUG)
#define LOG_TRACE_ENABLED (LOG_LEVEL >= LOG_LEVEL_TRACE)

#if LOG_FATAL_ENABLED
#define LOG_FATAL(format, args...) \
  do { \
    log_msg("FATAL", __FILE__, __LINE__, __FUNCTION__, ##args); \
  } while (0)
#else /* LOG_FATAL_ENABLED */
#define LOG_FATAL(args...) /* nothing */
#endif /* LOG_FATAL_ENABLED */

#if LOG_ERROR_ENABLED
#define LOG_ERROR(args...) \
  do { \
    log_msg("ERROR", __FILE__, __LINE__, __FUNCTION__, ##args); \
  } while (0);\
  fflush(stdout)
#else /* LOG_ERROR_ENABLED */
#define LOG_ERROR(args...) /* nothing */
#endif /* LOG_ERROR_ENABLED */

#if LOG_WARN_ENABLED
#define LOG_WARN(args...) \
  do { \
    log_msg("WARN", __FILE__, __LINE__, __FUNCTION__, ##args); \
  } while (0)
#else /* LOG_WARN_ENABLED */
#define LOG_WARN(args...) /* nothing */
#endif /* LOG_WARN_ENABLED */

#if LOG_INFO_ENABLED
#define LOG_INFO(args...) \
  do { \
    log_msg("INFO", __FILE__, __LINE__, __FUNCTION__, ##args); \
  } while (0)
#else /* LOG_INFO_ENABLED */
#define LOG_INFO(args...) /* nothing */
#endif /* LOG_INFO_ENABLED */

#if LOG_DEBUG_ENABLED
#define LOG_DEBUG(args...) \
  do { \
    log_msg("DEBUG", __FILE__, __LINE__, __FUNCTION__, ##args); \
  } while (0)
#else /* LOG_DEBUG_ENABLED */
#define LOG_DEBUG(args...) /* nothing */
#endif /* LOG_DEBUG_ENABLED */

#if LOG_TRACE_ENABLED
#define LOG_TRACE(args...) \
  do { \
    log_msg("TRACE", __FILE__, __LINE__, __FUNCTION__, ##args); \
  } while (0)
#else /* LOG_TRACE_ENABLED */
#define LOG_TRACE(args...) /* nothing */
#endif /* LOG_TRACE_ENABLED */


#include <string.h>
#include <stdarg.h>
#include <stdlib.h>

#include "mtime.h"

static void log_msg(const char *sz_level, const char* file, int line,
		const char* func, const char* fmt, ...) {
    // printf is thread safe.
    apr_time_t t = apr_time_now();
    int ti = (int)(t / 1000);
    char *buf = (char*) calloc((strlen(fmt) + 30), sizeof(char));
    char timebuf[20];
    sprintf(timebuf, "%d", ti);
    strcat(buf, sz_level);
    strcat(buf, " ");
    strcat(buf, timebuf);
    strcat(buf, " ");
    strcat(buf, fmt);
    strcat(buf, "\n");

	va_list args;
	va_start(args, fmt);
	vprintf(buf, args);
	va_end(args);
    free(buf);
}

#endif /* LOGGER_H_ */
