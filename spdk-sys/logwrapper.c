#include "logwrapper.h"

void
maya_log(int level, const char *file, const int line, const char *func,
    const char *format, va_list args)
{
    char buf[1024] = {0};
    unsigned int would_have_written = vsnprintf(buf, sizeof(buf), format, args);
    logfn(level, file, line, func, &buf[0], ((would_have_written > sizeof(buf)) ? sizeof(buf) : would_have_written));
}

