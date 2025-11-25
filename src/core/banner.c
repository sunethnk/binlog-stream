#include "banner.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#ifndef BINLOG_STREAMER_NAME
#define BINLOG_STREAMER_NAME "BINLOG STREAMER"
#endif

#ifndef BINLOG_STREAMER_VERSION
#define BINLOG_STREAMER_VERSION "dev"
#endif

#ifndef BINLOG_STREAMER_BUILD
#define BINLOG_STREAMER_BUILD "local"
#endif

/* Banner style enum */
typedef enum {
    BANNER_MINIMAL = 0,
    BANNER_ASCII,
    BANNER_WIDE,
    BANNER_WAVEFORM,
    BANNER_COUNT
} banner_style_t;

/* Detect terminal color support */
static int banner_use_color(void) {
    return isatty(STDERR_FILENO);
}

static const char* c_reset = "";
static const char* c_cyan  = "";
static const char* c_gray  = "";
static const char* c_green = "";

/* ---------------- STYLES ---------------- */

static void banner_minimal(void) {
    fprintf(stderr,
        "%s%s v%s%s (%s)\n"
        "%sPID:%s %d\n\n",
        c_cyan, BINLOG_STREAMER_NAME, BINLOG_STREAMER_VERSION, c_reset,
        BINLOG_STREAMER_BUILD,
        c_gray, c_reset, getpid());
}

static void banner_ascii(void) {
    fprintf(stderr, "%s"
"██████╗ ██╗███╗   ██╗██╗      ██████╗  ██████╗ \n"
"██╔══██╗██║████╗  ██║██║     ██╔═══██╗██╔════╝ \n"
"██████╔╝██║██╔██╗ ██║██║     ██║   ██║██║  ███╗\n"
"██╔═══╝ ██║██╔╚██╗██║██║     ██║   ██║██║   ██║\n"
"██║     ██║██║ ╚████║███████╗╚██████╔╝╚██████╔╝\n"
"╚═╝     ╚═╝╚═╝  ╚═══╝╚══════╝ ╚═════╝  ╚═════╝ \n"
"%s",
        c_cyan, c_reset);

    fprintf(stderr,
"%sVersion:%s %s\n"
"%sBuild  :%s %s\n"
"%sPID    :%s %d\n\n",
        c_gray, c_reset, BINLOG_STREAMER_VERSION,
        c_gray, c_reset, BINLOG_STREAMER_BUILD,
        c_gray, c_reset, getpid());
}

static void banner_wide(void) {
    fprintf(stderr,
"%s========================================================================================================================%s\n"
"%s %-50s %s v%-10s %s build:%s %s\n"
"%s PID:%s %d\n"
"%s========================================================================================================================%s\n\n",
        c_cyan, c_reset,
        c_green, BINLOG_STREAMER_NAME, c_reset, BINLOG_STREAMER_VERSION,
        c_gray, c_reset, BINLOG_STREAMER_BUILD,
        c_gray, c_reset, getpid(),
        c_cyan, c_reset);
}

static void banner_waveform(void) {
    fprintf(stderr, "%s"
"▁▂▃▅▆▇█ STREAMING BINLOG EVENTS █▇▆▅▃▂▁\n"
"%s Version %s (%s)\n"
"%s PID %d%s\n\n",
        c_cyan,
        c_gray, BINLOG_STREAMER_VERSION, BINLOG_STREAMER_BUILD,
        c_gray, getpid(), c_reset);
}

/* ---------------- STYLE SELECTION ---------------- */

static banner_style_t select_style(void) {
#ifdef BANNER_STYLE
    return (banner_style_t)BANNER_STYLE;
#endif

    const char *env = getenv("BINLOG_BANNER_STYLE");
    if (env) {
        int v = atoi(env);
        if (v >= 0 && v < BANNER_COUNT) return (banner_style_t)v;
    }

    srand(time(NULL));
    return (banner_style_t)(rand() % BANNER_COUNT);
}

/* ---------------- PUBLIC ENTRY ---------------- */

void binlog_print_banner(void) {
    if (banner_use_color()) {
        c_reset = "\x1b[0m";
        c_cyan  = "\x1b[96m";
        c_gray  = "\x1b[90m";
        c_green = "\x1b[92m";
    }

    switch (select_style()) {
        case BANNER_MINIMAL:  banner_minimal();  break;
        case BANNER_ASCII:    banner_ascii();    break;
        case BANNER_WIDE:     banner_wide();     break;
        case BANNER_WAVEFORM: banner_waveform(); break;
        default:              banner_ascii();    break;
    }
}
