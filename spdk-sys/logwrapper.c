#include "wrapper.h"

void
maya_log(int level, const char *file, const int line, const char *func,
    const char *format, va_list args)
{
    char buf[1024] = {0};
    int n_written = vsnprintf(buf, sizeof(buf), format, args);
    logfn(level, file, line, func, &buf[0], n_written);
}

