#include "logwrapper.h"
#include <stdio.h>
#include <stdlib.h>

void
maya_log(int level, const char *file, const int line, const char *func,
    const char *format, va_list args)
{
    // There is a delicate balance here! This `buf` ideally should not be resized, since a heap alloc is expensive.
    char buf[4096] = {0};
    int should_have_written = vsnprintf(buf, sizeof(buf), format, args);

    if (should_have_written > (int) sizeof(buf)) {
        logfn(level, file, line, func, &buf[0], sizeof(buf));
    } else {
        // If `should_have_written` is bigger than `buf`, then the message is too long.
        // Instead, we'll try to malloc onto the heap and log with that instead.
        char *dynamic_buf = malloc(should_have_written);
        if (!dynamic_buf) {
            // We are out of memory. Trying to allocate more is not going to work out ok.
            // Since C strings need `\0` on the end, we'll do that.
            buf[sizeof(buf) - 1] = '\0';
            logfn(level, file, line, func, &buf[0], sizeof(buf));
        } else {
            vsnprintf(dynamic_buf, should_have_written, format, args);
            logfn(level, file, line, func, &dynamic_buf[0], sizeof(dynamic_buf));
            free(dynamic_buf);
        }
    }
}

