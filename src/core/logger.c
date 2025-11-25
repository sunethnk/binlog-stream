/*
 * Copyright (c) 2020 rxi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>

#define MAX_CALLBACKS 32

typedef struct {
  log_LogFn fn;
  void *udata;
  int level;
} Callback;

static struct {
  void *udata;
  log_LockFn lock;
  int level;
  bool quiet;
  Callback callbacks[MAX_CALLBACKS];
} L;

static const char *level_strings[] = {
  "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"
};

#ifdef LOG_USE_COLOR
static const char *level_colors[] = {
  "\x1b[94m", "\x1b[36m", "\x1b[32m", "\x1b[33m", "\x1b[31m", "\x1b[35m"
};
#endif

/* ------------------------------------------------------------------------- */
/* Internal helpers                                                          */
/* ------------------------------------------------------------------------- */

static void lock(void) {
  if (L.lock) {
    L.lock(true, L.udata);
  }
}

static void unlock(void) {
  if (L.lock) {
    L.lock(false, L.udata);
  }
}

const char* log_level_string(int level) {
  if (level < LOG_TRACE || level > LOG_FATAL) return "UNKNOWN";
  return level_strings[level];
}

void log_set_lock(log_LockFn fn, void *udata) {
  L.lock = fn;
  L.udata = udata;
}

void log_set_level(int level) {
  L.level = level;
}

void log_set_quiet(bool enable) {
  L.quiet = enable;
}

int log_add_callback(log_LogFn fn, void *udata, int level) {
  for (int i = 0; i < MAX_CALLBACKS; i++) {
    if (!L.callbacks[i].fn) {
      L.callbacks[i].fn = fn;
      L.callbacks[i].udata = udata;
      L.callbacks[i].level = level;
      return 0;
    }
  }
  return -1;
}

/* ------------------------------------------------------------------------- */
/* Stdout / file callbacks (original rxi logger)                             */
/* ------------------------------------------------------------------------- */

static void stdout_callback(log_Event *ev) {
    char buf[64];
    buf[strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ev->time)] = '\0';

#ifdef LOG_USE_COLOR
    /* Timestamp + colored level, then " - " */
    fprintf(
        ev->udata,
        "[%s.%03ld] %s[%-5s]\x1b[0m - ",
        buf,
        ev->nsec / 1000000,
        level_colors[ev->level],
        level_strings[ev->level]
    );

  #ifdef DEVELOPER_MODE
    /* Grey file:line: with trailing space */
    fprintf(
        ev->udata,
        "\x1b[90m%s:%d:\x1b[0m ",
        ev->file,
        ev->line
    );
  #endif

#else /* !LOG_USE_COLOR */

    /* Timestamp + level in brackets, then " - " */
    fprintf(
        ev->udata,
        "[%s.%03ld] [%-5s] - ",
        buf,
        ev->nsec / 1000000,
        level_strings[ev->level]
    );

  #ifdef DEVELOPER_MODE
    /* file:line: with trailing space */
    fprintf(
        ev->udata,
        "%s:%d: ",
        ev->file,
        ev->line
    );
  #endif

#endif /* LOG_USE_COLOR */

    /* Message and newline */
    vfprintf(ev->udata, ev->fmt, ev->ap);
    fprintf(ev->udata, "\n");
    fflush(ev->udata);
}

static void file_callback(log_Event *ev) {
  char buf[64];
  buf[strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ev->time)] = '\0';
  fprintf(
    ev->udata,
    "[%s.%03ld] [%-5s] ",
    buf, ev->nsec / 1000000,
    level_strings[ev->level]
  );
#ifdef DEVELOPER_MODE
  fprintf(
    ev->udata,
    " - %s:%d: ",
    ev->file, ev->line
  );
#else
  fprintf(
    ev->udata,
    " - "
  );
#endif
  vfprintf(ev->udata, ev->fmt, ev->ap);
  fprintf(ev->udata, "\n");
  fflush(ev->udata);
}

int log_add_fp(FILE *fp, int level) {
  return log_add_callback(file_callback, fp, level);
}

/* ------------------------------------------------------------------------- */
/* Time initialisation with nsec                                             */
/* ------------------------------------------------------------------------- */

static void init_event(log_Event *ev, void *udata) {
  if (!ev->time) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    ev->nsec = ts.tv_nsec;

    static struct tm tm_buf;
    localtime_r(&ts.tv_sec, &tm_buf);
    ev->time = &tm_buf;
  }
  ev->udata = udata;
}

/* ------------------------------------------------------------------------- */
/* Rotating file support                                                     */
/* ------------------------------------------------------------------------- */

static void rotate_files(log_file_t *lf) {
  if (!lf || !lf->path[0] || lf->max_files <= 0) return;

  if (lf->fp) {
    fclose(lf->fp);
    lf->fp = NULL;
  }

  char oldname[512];
  char newname[512];

//  /* Shift existing rotated files: path.(n-1) -> path.n */
//  for (int i = lf->max_files - 1; i >= 1; i--) {
//    snprintf(oldname, sizeof(oldname), "%s.%d", lf->path, i - 1);
//    snprintf(newname, sizeof(newname), "%s.%d", lf->path, i);
//    rename(oldname, newname); /* ignore errors */
//  }
  
  /* Shift existing rotated files: path.(n-1) -> path.n */
    for (int i = lf->max_files - 1; i >= 1; i--) {

        /* leave room for ".<index>\0" — assume max 10 digits */
        size_t space = sizeof(oldname);
        size_t max_base = space - 12;  // '.' + up to 10 digits + '\0'

        size_t plen = strnlen(lf->path, sizeof(lf->path));
        if (plen > max_base) plen = max_base;

        /* oldname = "path.(i-1)" */
        snprintf(oldname, sizeof(oldname), "%.*s.%d",
                 (int)plen, lf->path, i - 1);

        /* newname = "path.i" */
        snprintf(newname, sizeof(newname), "%.*s.%d",
                 (int)plen, lf->path, i);

        rename(oldname, newname); /* ignore errors */
    }


  /* Move current base file to .0 */
  size_t maxo = sizeof(oldname) - 4; // '.', digit, '\0'
  snprintf(oldname, sizeof(oldname), "%.*s.%d",
         (int)maxo, lf->path, 0);
  //snprintf(oldname, sizeof(oldname), "%s", lf->path);
  
  size_t maxn = sizeof(newname) - 4; // '.', digit, '\0'
  snprintf(newname, sizeof(newname), "%.*s.%d",
         (int)maxn, lf->path, 0);
  
  //snprintf(newname, sizeof(newname), "%s.%d", lf->path, 0);
  rename(oldname, newname); /* ignore errors */

  /* Reopen base log file */
  lf->fp = fopen(lf->path, "a");
  if (!lf->fp) {
    lf->cur_bytes = 0;
    return;
  }

  lf->cur_bytes = 0;
}

static void rotating_file_callback(log_Event *ev) {
    log_file_t *lf = (log_file_t *) ev->udata;
    if (!lf || !lf->fp) return;

    char timebuf[64];
    timebuf[strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", ev->time)] = '\0';

    char line[4096];

    /* First part: timestamp + level */
    int n = snprintf(
        line, sizeof(line),
        "[%s.%03ld] [%-5s]",
        timebuf,
        ev->nsec / 1000000,
        level_strings[ev->level]
    );

    if (n < 0) n = 0;
    if ((size_t)n >= sizeof(line)) n = (int)(sizeof(line) - 1);

    /* Second part: either " file:line: " or just ": " */
    int k;
#ifdef DEVELOPER_MODE
    k = snprintf(
        line + n, sizeof(line) - (size_t)n,
        " - %s:%d: ",
        ev->file, ev->line
    );
#else
    k = snprintf(
        line + n, sizeof(line) - (size_t)n,
        " - "
    );
#endif

    if (k < 0) k = 0;
    if ((size_t)n + (size_t)k >= sizeof(line)) {
        k = (int)(sizeof(line) - 1 - (size_t)n);
    }
    n += k;

    /* Now append the formatted message */
    int m = vsnprintf(line + n, sizeof(line) - (size_t)n, ev->fmt, ev->ap);
    if (m < 0) m = 0;

    size_t len = (size_t)n + (size_t)m;
    if (len >= sizeof(line) - 2) {
        len = sizeof(line) - 2;
    }

    line[len++] = '\n';
    line[len] = '\0';

    /* Rotation check */
    if (lf->max_bytes > 0 && lf->cur_bytes + len > lf->max_bytes) {
        rotate_files(lf);
        if (!lf->fp) return;
    }

    size_t written = fwrite(line, 1, len, lf->fp);
    if (written > 0) {
        lf->cur_bytes += written;
    }
    fflush(lf->fp);
}


int log_add_rotating_file(log_file_t *lf,
                          const char *path,
                          size_t max_bytes,
                          int max_files,
                          int level) {
  if (!lf || !path || max_files <= 0) {
    return -1;
  }

  memset(lf, 0, sizeof(*lf));
  strncpy(lf->path, path, sizeof(lf->path) - 1);
  lf->path[sizeof(lf->path) - 1] = '\0';

  lf->max_bytes = max_bytes;
  lf->max_files = max_files;

  lf->fp = fopen(path, "a");
  if (!lf->fp) {
    return -1;
  }

  /* Initialise current size from existing file */
  if (fseek(lf->fp, 0, SEEK_END) == 0) {
    long pos = ftell(lf->fp);
    if (pos > 0) {
      lf->cur_bytes = (size_t) pos;
    }
  }

  return log_add_callback(rotating_file_callback, lf, level);
}

void log_close_file(log_file_t *lf) {
  if (!lf) return;
  if (lf->fp) {
    fclose(lf->fp);
    lf->fp = NULL;
  }
}

/* ------------------------------------------------------------------------- */
/* Public logging API                                                        */
/* ------------------------------------------------------------------------- */

void log_log(int level, const char *file, int line, const char *fmt, ...) {
  log_Event ev;
  ev.fmt   = fmt;
  ev.file  = file;
  ev.line  = line;
  ev.level = level;
  ev.time  = NULL;
  ev.nsec  = 0;
  ev.udata = NULL;

  lock();

  if (!L.quiet && level >= L.level) {
    init_event(&ev, stderr);
    va_start(ev.ap, fmt);
    stdout_callback(&ev);
    va_end(ev.ap);
  }

  for (int i = 0; i < MAX_CALLBACKS && L.callbacks[i].fn; i++) {
    Callback *cb = &L.callbacks[i];
    if (level >= cb->level) {
      init_event(&ev, cb->udata);
      va_start(ev.ap, fmt);
      cb->fn(&ev);
      va_end(ev.ap);
    }
  }

  unlock();
}
