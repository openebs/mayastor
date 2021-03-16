#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>

void run_fio_sh(char** argv) {
    char *cmd = NULL;
    /* Tis' C so we do it the "hard way" */
    char *pinsert;
    const char *prefix = "fio ";
    /* stop fio generatng curses output,  stdio is not a tty */
    const char *suffix = " | cat";
    size_t buflen = strlen(prefix) + strlen(suffix) + 1;
    /* 1. work out the size of the buffer required to copy the arguments.*/
    for(char **argv_scan=argv; *argv_scan != NULL; ++argv_scan) {
        /* +1 for space delimiter */
        buflen += strlen(*argv_scan) + 1;
    }
    /* 2. allocate a 0 intialised buffer so we can use strcat */
    cmd = calloc(sizeof(unsigned char), buflen);
    pinsert = cmd;
    /* 3. construct the command line, using strcat */
    if (cmd != NULL) {
        strcat(pinsert, prefix);
        pinsert += strlen(pinsert);
        for(; *argv != NULL; ++argv) {
            strcat(pinsert, *argv);
            pinsert += strlen(pinsert);
            *pinsert = ' ';
            ++pinsert;
        }
        strcat(pinsert,suffix);
        printf("exec %s\n",cmd);
        execl("/bin/sh", "sh", "-c", cmd, NULL);
        free(cmd);
    } else {
        puts("failed to allocate memory");
    }
}

/*
 * Usage:
 * [sleep <sleep seconds>] [segfault-after <delay seconds>] [-- <fio argument list>] [exitv <exit value>] 
 * 1. fio is only run if fio arguments are specified.
 * 2. fio is always run as a forked process.
 * 3. the segfault directive takes priority over the sleep directive
 * 4. exitv <v> override exit value - this is to simulate failure in the test pod.
 * 5. argument parsing is simple, invalid specifications are skipped over
 *  for example "sleep --" => sleep is skipped over, parsing resumes from "--"
 */
int main(int argc, char **argv_in)
{
    unsigned sleep_time = 0;
    unsigned segfault_time = 0;
    char** argv = argv_in;
    pid_t   fio_pid = 0;
    int     running_fio = 0;
    int     exitv = 0;

    /* skip over this programs name */
    argv += 1;
    /* "parse" arguments -
     * 1. segfault-after <n> number of seconds
     * 2. sleep <n> number of seconds
     * 3. anything after "--" is merely collected and passed to fio
     *    -- also implies fio is launched.
     * 4. exitv <v> override exit value - this is to aid test development.
     *      specifically to validate error detection in the tests.
     * For our simple purposes atoi is sufficient 
     *
     * For simplicity none of the arguments are mandatory
     * if no arguments are supplied execution ends
     * segfault-after is always handled before sleep
     * fio is always run as a forked process so executes concurrently
     * if fio is launched, we wait for it complete and return its exit value.
     * Note you can use --status-interval=N as an argument to get fio to print status every N seconds
     *
     * Intended use cases are
     * a) sleep N fio is executed using exec 
     * b) segfault-after N, sleep for N then segfault terminating the pod or restarting the pod
     * c) segfault-after N -- ...., run fio (in a different process) and segfault after N seconds
     * d) -- ....., run fio, if fio completes, execution ends.
     */
    while(*argv != NULL) {
        if (0 == strcmp(*argv,"sleep") && NULL != *(argv+1) && 0 != atoi(*(argv+1))) {
            sleep_time = atoi(*(argv+1));
            ++argv;
        } else if (0 == strcmp(*argv,"segfault-after") && NULL != *(argv+1) && 0 != atoi(*(argv+1))) {
            segfault_time = atoi(*(argv+1));
            ++argv;
        } else if (0 == strcmp(*argv, "--")) {
            ++argv;
            break;
        } else if (0 == strcmp(*argv,"exitv") && NULL != *(argv+1) && 0 != atoi(*(argv+1))) {
            exitv = atoi(*(argv+1));
            printf("Overriding exit value to %d\n", exitv);
            ++argv;
        } else {
            printf("Ignoring %s\n", *argv);
        }
        ++argv;
    }

    /* fio arguments have been supplied */
    if (*argv != NULL) {
        fio_pid = fork();
        if ( 0 == fio_pid ) {
            run_fio_sh(argv);
            exit(0);
        } else {
            running_fio = 1;
        }
    }

    /* segfault has priority over sleep */
    if (0 != segfault_time) {
        printf("Segfaulting after %d seconds\n", segfault_time);
        sleep(segfault_time);
        if (0 != fio_pid) {
            system("killall fio");
            kill(fio_pid, SIGKILL);
            sleep(1);
        }
        puts("Segfaulting now!");
        raise(SIGSEGV);
    }

    if (0 != sleep_time) {
        printf("sleeping %d seconds\n", sleep_time);
        sleep(sleep_time);
    }

    /* if fio was launched wait for it to complete */
    if (0 != running_fio) {
        int status;
        waitpid(fio_pid, &status, 0);
        if (exitv == 0) {
            printf("Exit value is fio status, %d\n", status);
            return status;
        }
    }

    printf("Exit value is %d\n", exitv);
    return exitv;
}
