#pragma once

namespace chfs {

/*
  All the error code expected by the chfs
*/
enum class ErrorType {
  DONE = 0,
  /** The argument is invalid.*/
  INVALID_ARG = 1,

  /** There is no resource left  */
  OUT_OF_RESOURCE = 2,
  
  /** Invalid exception type.*/
  INVALID = 3,

  /** The resource is not presented */
  NotExist = 4,

  /* The resource is already presented */
  AlreadyExist = 5,

  NotEmpty = 6,
};

} // namespace chfs