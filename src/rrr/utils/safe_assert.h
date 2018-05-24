/*
 * safe_assert.h
 *
 *  Created on: Nov 9, 2012
 *      Author: frog
 */

#ifndef SAFE_ASSERT_H_
#define SAFE_ASSERT_H_

#include <stdlib.h>
#include <assert.h>

#ifdef NDEBUG
#define SAFE_ASSERT(expr) \
  do { \
    if (!(expr)) { \
      abort(); \
    } \
  } while (0)
#else /* NDEBUG */
#define SAFE_ASSERT(expr) assert(expr)
#endif /* NDEBUG */

#endif /* SAFE_ASSERT_H_ */
