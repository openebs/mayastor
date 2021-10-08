#include "logwrapper.h"
#include <stdio.h>
#include <stdlib.h>

void
spdk_rs_log(int level, const char *file, const int line, const char *func,
    const char *format, va_list args)
{
    char buf[512] = {0};
    vsnprintf(buf, sizeof(buf), format, args);
    logfn(level, file, line, func, &buf[0], sizeof(buf));
}
