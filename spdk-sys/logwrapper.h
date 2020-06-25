#include <stddef.h>
#include <stdarg.h>
#include <spdk/log.h>

typedef void maya_logger(int level, const char *file, const int line,
    const char *func, const char *buf, const int len);

// pointer is set from within rust to point to our logging trampoline
maya_logger *logfn = NULL;

