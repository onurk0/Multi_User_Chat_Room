/*
 * net_common.h - Socket helpers shared by the chat server and client.
 *
 * Every message on the wire is a '\n'-terminated text line, except file
 * data, which travels as a "[FILE_CHUNK] <n>\n" line followed by exactly n
 * raw bytes. TCP is a byte stream, so a single recv() may return half a
 * message or several messages glued together. Conn wraps a socket with a
 * read buffer so callers can pull either one whole line or an exact number
 * of bytes from the stream, regardless of how the data was split in transit.
 */
#ifndef NET_COMMON_H
#define NET_COMMON_H

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

// Ports below 1024 need root; build with -DPORT_NUM=<port> to use another.
#ifndef PORT_NUM
#define PORT_NUM 1004
#endif
#define CHUNK_SIZE 4096  // max payload of a single [FILE_CHUNK]
#define LINE_SIZE 512    // max length of one text line (extra is dropped)
#define NAME_SIZE 50     // max username length, including '\0'
#define FILENAME_SIZE 256

// A socket plus a buffer of bytes received but not yet consumed.
typedef struct {
  int fd;
  char buf[CHUNK_SIZE];
  size_t start; // index of the first unread byte in buf
  size_t end;   // one past the last unread byte in buf
} Conn;

static inline void conn_init(Conn *c, int fd) {
  c->fd = fd;
  c->start = c->end = 0;
}

// Refills the (fully consumed) buffer from the socket.
// Returns >0 on success, 0 on EOF, <0 on error or timeout.
static inline ssize_t conn_fill(Conn *c) {
  ssize_t n;
  c->start = c->end = 0;
  do {
    n = recv(c->fd, c->buf, sizeof(c->buf), 0);
  } while (n < 0 && errno == EINTR);
  if (n > 0)
    c->end = (size_t)n;
  return n;
}

// Reads one line into out (without the trailing "\n" or "\r\n").
// Returns the line length, or -1 if the connection closed first.
static inline int conn_read_line(Conn *c, char *out, size_t out_size) {
  size_t len = 0;
  for (;;) {
    while (c->start < c->end) {
      char ch = c->buf[c->start++];
      if (ch == '\n') {
        out[len] = '\0';
        return (int)len;
      }
      if (ch != '\r' && len < out_size - 1)
        out[len++] = ch;
    }
    if (conn_fill(c) <= 0)
      return -1;
  }
}

// Reads exactly n bytes into out. Returns 0 on success, -1 on EOF/error.
static inline int conn_read_exact(Conn *c, void *out, size_t n) {
  char *dst = out;
  while (n > 0) {
    if (c->start == c->end && conn_fill(c) <= 0)
      return -1;
    size_t avail = c->end - c->start;
    if (avail > n)
      avail = n;
    memcpy(dst, c->buf + c->start, avail);
    c->start += avail;
    dst += avail;
    n -= avail;
  }
  return 0;
}

// Sends all len bytes, retrying on short writes.
// Returns 0 on success, -1 on error. MSG_NOSIGNAL keeps a closed peer from
// killing the process with SIGPIPE.
static inline int send_all(int fd, const void *buf, size_t len) {
  const char *p = buf;
  while (len > 0) {
    ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return -1;
    }
    p += n;
    len -= (size_t)n;
  }
  return 0;
}

// printf-style formatting into buf. Returns the formatted length, clamped to
// fit buf if the output was truncated.
static inline size_t format_msg(char *buf, size_t size, const char *fmt,
                                va_list ap) {
  int len = vsnprintf(buf, size, fmt, ap);
  if (len < 0)
    return 0;
  return (size_t)len < size ? (size_t)len : size - 1;
}

// If s starts with prefix, returns the rest of s with leading spaces
// skipped; otherwise returns NULL.
static inline const char *after_prefix(const char *s, const char *prefix) {
  size_t n = strlen(prefix);
  if (strncmp(s, prefix, n) != 0)
    return NULL;
  s += n;
  while (*s == ' ')
    s++;
  return s;
}

#endif
