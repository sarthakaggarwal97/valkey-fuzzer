/* An LD_PRELOAD library that delays, fails, or kills its process at the nth matching write, writev, fsync,
 * fdatasync or rename: a fault placed between two system calls, where a kill from outside cannot land.
 *
 * The local deployment starts a server with it when the deployment sets syscall_faults; the variable
 * VALKEY_FUZZ_SYSCALL_FAULTS names the instance's rule directory. valkey.fault.syscall writes each rule there as
 * <id>.rule (atomically, by rename) while the server runs. The library reads the directory again when it changed,
 * at most once a millisecond and only inside a call it intercepts. A rule is one line of key=value fields:
 *
 *   calls=write,fsync   the calls it matches: write (write and writev), fsync, fdatasync, rename
 *   path=GLOB           the file's path matches the fnmatch pattern: an fd's path from /proc/self/fd, either path
 *                       of a rename (made absolute against the working directory)
 *   peer_ports=P,Q      the fd is a socket whose peer's port is one of these
 *   nth=N               act on the Nth matching call since the rule was read, counted across the process and its
 *                       fork children (an RDB or AOF rewrite child); a restarted server counts afresh
 *   action=kill|delay|fail
 *                       kill: SIGKILL the server and, in a fork child, the child too: the server crashes at that
 *                       point (a process crash: what the kernel already holds of its writes survives)
 *   when=before|after   kill or delay before the call, or once it returned
 *   delay_ms=N          for delay
 *   errno=N             for fail: return -1 with this errno, without making the call
 *
 * Each matching call up to the nth appends a JSON line to <id>.log in the same directory; the nth names its
 * action. Before it acts, the rule is renamed to <id>.fired, so no process (a restarted server included) acts on
 * it again. Without the variable, or with no rule, every call goes straight through.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

enum { CALL_WRITE = 1, CALL_FSYNC = 2, CALL_FDATASYNC = 4, CALL_RENAME = 8 };
enum { ACT_KILL, ACT_DELAY, ACT_FAIL };
#define MAX_RULES 16
/* Rules a server reads in its lifetime. */
#define MAX_SLOTS 64
#define MAX_PORTS 8
#define ID_LEN 96
#define PATH_LEN 1024

struct rule {
    char id[ID_LEN];
    int calls;
    char path[PATH_LEN]; /* empty: any path */
    int ports[MAX_PORTS];
    int nports;
    unsigned long nth;
    int action;
    int after;
    long delay_ms;
    int err;
};

/* Counts shared by the process and its fork children: a mapping made before any fork. */
struct shared {
    pthread_mutex_t lock; /* robust: a process killed holding it does not block the others */
    struct {
        char id[ID_LEN];
        unsigned long seen;
        int done;
    } slots[MAX_SLOTS];
};

static ssize_t (*real_write)(int, const void *, size_t);
static ssize_t (*real_writev)(int, const struct iovec *, int);
static int (*real_fsync)(int);
static int (*real_fdatasync)(int);
static int (*real_rename)(const char *, const char *);

static char rule_dir[PATH_LEN];
/* The server: the process the library was loaded into; its fork children inherit it. */
static pid_t server_pid;
static struct shared *shared;
static struct rule rules[MAX_RULES];
static int nrules;
static pthread_rwlock_t rules_lock = PTHREAD_RWLOCK_INITIALIZER;
static uint64_t last_check_ns;
static struct timespec seen_mtime;
static int seen_settled;

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000u + (uint64_t)ts.tv_nsec;
}

static void resolve(void) {
    real_write = dlsym(RTLD_NEXT, "write");
    real_writev = dlsym(RTLD_NEXT, "writev");
    real_fsync = dlsym(RTLD_NEXT, "fsync");
    real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    real_rename = dlsym(RTLD_NEXT, "rename");
}

/* A fork while another thread holds rules_lock (an IO thread reloading the rules while the main thread forks an
 * RDB or AOF-rewrite child) would leave the child a lock no thread of its own can release, and its next intercepted
 * call would hang it. The forking thread takes the lock across the fork, so both sides start with it free. */
static void fork_prepare(void) {
    pthread_rwlock_wrlock(&rules_lock);
}

static void fork_parent(void) {
    pthread_rwlock_unlock(&rules_lock);
}

/* The child's thread has a new tid, and glibc tells a writer's unlock from a reader's by the tid: start afresh. */
static void fork_child(void) {
    pthread_rwlock_init(&rules_lock, NULL);
}

__attribute__((constructor)) static void init(void) {
    resolve();
    const char *dir = getenv("VALKEY_FUZZ_SYSCALL_FAULTS");
    if (!dir || !*dir || strlen(dir) >= sizeof(rule_dir) - ID_LEN - 8) return;
    struct shared *mapped = mmap(NULL, sizeof(*shared), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (mapped == MAP_FAILED) return;
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&mapped->lock, &attr);
    strcpy(rule_dir, dir);
    server_pid = getpid();
    shared = mapped;
    pthread_atfork(fork_prepare, fork_parent, fork_child);
}

static int parse_list(const char *value, int *out, int max) {
    int count = 0;
    while (*value && count < max) {
        out[count++] = atoi(value);
        const char *comma = strchr(value, ',');
        if (!comma) break;
        value = comma + 1;
    }
    return count;
}

/* One rule from its file's text; 0 when a field is unknown or a value is missing. */
static int parse_rule(char *text, struct rule *rule) {
    memset(rule, 0, sizeof(*rule));
    rule->nth = 1;
    for (char *save = NULL, *field = strtok_r(text, " \t\n", &save); field; field = strtok_r(NULL, " \t\n", &save)) {
        char *value = strchr(field, '=');
        if (!value) return 0;
        *value++ = '\0';
        if (!strcmp(field, "calls")) {
            for (char *rest = NULL, *name = strtok_r(value, ",", &rest); name; name = strtok_r(NULL, ",", &rest)) {
                if (!strcmp(name, "write")) rule->calls |= CALL_WRITE;
                else if (!strcmp(name, "fsync")) rule->calls |= CALL_FSYNC;
                else if (!strcmp(name, "fdatasync")) rule->calls |= CALL_FDATASYNC;
                else if (!strcmp(name, "rename")) rule->calls |= CALL_RENAME;
                else return 0;
            }
        } else if (!strcmp(field, "path")) {
            snprintf(rule->path, sizeof(rule->path), "%s", value);
        } else if (!strcmp(field, "peer_ports")) {
            rule->nports = parse_list(value, rule->ports, MAX_PORTS);
        } else if (!strcmp(field, "nth")) {
            rule->nth = strtoul(value, NULL, 10);
        } else if (!strcmp(field, "action")) {
            if (!strcmp(value, "kill")) rule->action = ACT_KILL;
            else if (!strcmp(value, "delay")) rule->action = ACT_DELAY;
            else if (!strcmp(value, "fail")) rule->action = ACT_FAIL;
            else return 0;
        } else if (!strcmp(field, "when")) {
            rule->after = !strcmp(value, "after");
        } else if (!strcmp(field, "delay_ms")) {
            rule->delay_ms = atol(value);
        } else if (!strcmp(field, "errno")) {
            rule->err = atoi(value);
        } else {
            return 0;
        }
    }
    return rule->calls && rule->nth;
}

/* Read the rule directory again if it changed. A second change within the timestamp granularity of the first
 * leaves the same mtime, so a directory read less than 50 ms after its mtime is read again at the next check. */
static void maybe_reload(void) {
    uint64_t now = monotonic_ns();
    if (now - __atomic_load_n(&last_check_ns, __ATOMIC_RELAXED) < 1000000u) return;
    __atomic_store_n(&last_check_ns, now, __ATOMIC_RELAXED);
    struct stat st;
    if (stat(rule_dir, &st) != 0) return;
    struct timespec wall;
    clock_gettime(CLOCK_REALTIME, &wall);
    int64_t age_ms = (wall.tv_sec - st.st_mtim.tv_sec) * 1000 + (wall.tv_nsec - st.st_mtim.tv_nsec) / 1000000;
    if (seen_settled && st.st_mtim.tv_sec == seen_mtime.tv_sec && st.st_mtim.tv_nsec == seen_mtime.tv_nsec) return;
    /* Another thread holds the rules: keep the ones this call has, and read the directory at a later check. */
    if (pthread_rwlock_trywrlock(&rules_lock) != 0) return;
    seen_mtime = st.st_mtim;
    seen_settled = age_ms > 50;
    int count = 0;
    int dir = open(rule_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir >= 0) {
        char buffer[8192];
        long got;
        while ((got = syscall(SYS_getdents64, dir, buffer, sizeof(buffer))) > 0) {
            for (long at = 0; at < got;) {
                struct dirent64 *entry = (struct dirent64 *)(buffer + at);
                at += entry->d_reclen;
                size_t len = strlen(entry->d_name);
                if (len < 6 || len - 5 >= ID_LEN || strcmp(entry->d_name + len - 5, ".rule") || count == MAX_RULES)
                    continue;
                int fd = openat(dir, entry->d_name, O_RDONLY | O_CLOEXEC);
                if (fd < 0) continue;
                char text[2048];
                ssize_t n = read(fd, text, sizeof(text) - 1);
                close(fd);
                if (n <= 0) continue;
                text[n] = '\0';
                if (!parse_rule(text, &rules[count])) continue;
                memcpy(rules[count].id, entry->d_name, len - 5);
                rules[count].id[len - 5] = '\0';
                count++;
            }
        }
        close(dir);
    }
    nrules = count;
    pthread_rwlock_unlock(&rules_lock);
}

/* The nth-count of rule id after this call: 0 once the rule acted (in any process), else the new count. */
static unsigned long count_call(const struct rule *rule) {
    int locked = pthread_mutex_lock(&shared->lock);
    if (locked == EOWNERDEAD) pthread_mutex_consistent(&shared->lock);
    else if (locked != 0) return 0;
    unsigned long seen = 0;
    for (int i = 0; i < MAX_SLOTS; i++) {
        if (!shared->slots[i].id[0]) strcpy(shared->slots[i].id, rule->id);
        if (strcmp(shared->slots[i].id, rule->id)) continue;
        if (!shared->slots[i].done) {
            seen = ++shared->slots[i].seen;
            if (seen >= rule->nth) shared->slots[i].done = 1;
        }
        break;
    }
    pthread_mutex_unlock(&shared->lock);
    return seen;
}

static void json_string(char *out, size_t size, const char *text) {
    size_t at = 0;
    for (; *text && at + 7 < size; text++) {
        unsigned char c = (unsigned char)*text;
        if (c == '"' || c == '\\') at += snprintf(out + at, size - at, "\\%c", c);
        else if (c < 0x20) at += snprintf(out + at, size - at, "\\u%04x", c);
        else out[at++] = (char)c;
    }
    out[at] = '\0';
}

static const char *call_name(int call) {
    switch (call) {
    case CALL_WRITE: return "write";
    case CALL_FSYNC: return "fsync";
    case CALL_FDATASYNC: return "fdatasync";
    default: return "rename";
    }
}

static const char *action_name(const struct rule *rule, int acting) {
    if (!acting) return "pass";
    return rule->action == ACT_KILL ? "kill" : rule->action == ACT_DELAY ? "delay" : "fail";
}

/* A rename's line has both paths: "path" and "to". */
static void log_call(const struct rule *rule, unsigned long n, int call, const char *path, const char *to, int acting) {
    char file[PATH_LEN + ID_LEN + 8], escaped[2 * PATH_LEN], escaped_to[2 * PATH_LEN + 16], line[5 * PATH_LEN];
    snprintf(file, sizeof(file), "%s/%s.log", rule_dir, rule->id);
    json_string(escaped, sizeof(escaped), path);
    escaped_to[0] = '\0';
    if (to[0]) {
        char quoted[2 * PATH_LEN];
        json_string(quoted, sizeof(quoted), to);
        snprintf(escaped_to, sizeof(escaped_to), ", \"to\": \"%s\"", quoted);
    }
    struct timespec wall;
    clock_gettime(CLOCK_REALTIME, &wall);
    int len = snprintf(line, sizeof(line),
                       "{\"n\": %lu, \"call\": \"%s\", \"path\": \"%s\"%s, \"pid\": %d, \"tid\": %ld, "
                       "\"t\": %ld.%06ld, \"action\": \"%s\", \"when\": \"%s\"}\n",
                       n, call_name(call), escaped, escaped_to, (int)getpid(), (long)syscall(SYS_gettid),
                       (long)wall.tv_sec, wall.tv_nsec / 1000, action_name(rule, acting),
                       rule->after ? "after" : "before");
    int fd = open(file, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return;
    if (len > 0) real_write(fd, line, (size_t)len < sizeof(line) ? (size_t)len : sizeof(line) - 1);
    close(fd);
}

static void retire(const struct rule *rule) {
    char from[PATH_LEN + ID_LEN + 8], to[PATH_LEN + ID_LEN + 8];
    snprintf(from, sizeof(from), "%s/%s.rule", rule_dir, rule->id);
    snprintf(to, sizeof(to), "%s/%s.fired", rule_dir, rule->id);
    real_rename(from, to);
}

static void fd_path(int fd, char *out, size_t size) {
    char link[64];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, out, size - 1);
    out[n > 0 ? n : 0] = '\0';
}

static void absolute(const char *path, char *out, size_t size) {
    if (path[0] == '/') {
        snprintf(out, size, "%s", path);
        return;
    }
    char cwd[PATH_LEN];
    if (!getcwd(cwd, sizeof(cwd))) cwd[0] = '\0';
    snprintf(out, size, "%s/%s", cwd, path);
}

static int peer_port(int fd) {
    struct sockaddr_storage address;
    socklen_t len = sizeof(address);
    if (getpeername(fd, (struct sockaddr *)&address, &len) != 0) return -1;
    if (address.ss_family == AF_INET) return ntohs(((struct sockaddr_in *)&address)->sin_port);
    if (address.ss_family == AF_INET6) return ntohs(((struct sockaddr_in6 *)&address)->sin6_port);
    return -1;
}

/* The rule that acts on this call, copied to *acting, or 0. Every matching rule counts the call. */
static int match(int call, int fd, const char *from, const char *to, struct rule *acting) {
    if (!shared) return 0;
    maybe_reload();
    if (!__atomic_load_n(&nrules, __ATOMIC_RELAXED)) return 0;
    if (pthread_rwlock_rdlock(&rules_lock) != 0) return 0;
    int found = 0, port = -2;
    char path[PATH_LEN] = "", other[PATH_LEN] = "";
    if (call == CALL_RENAME) {
        absolute(from, path, sizeof(path));
        absolute(to, other, sizeof(other));
    } else {
        fd_path(fd, path, sizeof(path));
    }
    for (int i = 0; i < nrules; i++) {
        const struct rule *rule = &rules[i];
        if (!(rule->calls & call)) continue;
        if (rule->path[0] && fnmatch(rule->path, path, 0) != 0 && (!other[0] || fnmatch(rule->path, other, 0) != 0))
            continue;
        if (rule->nports) {
            if (port == -2) port = call == CALL_RENAME ? -1 : peer_port(fd);
            int listed = 0;
            for (int p = 0; p < rule->nports; p++) listed |= rule->ports[p] == port;
            if (!listed) continue;
        }
        unsigned long n = count_call(rule);
        if (!n) continue;
        /* When two rules reach their nth on one call, the first read acts. */
        int acts = n == rule->nth && !found;
        log_call(rule, n, call, path, other, acts);
        if (acts) {
            *acting = *rule;
            found = 1;
        }
    }
    pthread_rwlock_unlock(&rules_lock);
    if (found) retire(acting);
    return found;
}

static void act(const struct rule *rule) {
    if (rule->action == ACT_KILL) {
        if (getpid() != server_pid) kill(server_pid, SIGKILL);
        kill(getpid(), SIGKILL);
        for (;;) pause();
    }
    struct timespec left = {rule->delay_ms / 1000, (rule->delay_ms % 1000) * 1000000};
    while (nanosleep(&left, &left) != 0 && errno == EINTR);
}

/* Run the call (real) under the rule that acts on it, if any. */
#define INTERCEPT(type, call, fd, from, to, real)                                                                      \
    do {                                                                                                               \
        struct rule rule;                                                                                              \
        int saved = errno;                                                                                             \
        if (!match(call, fd, from, to, &rule)) {                                                                       \
            errno = saved;                                                                                             \
            return real;                                                                                               \
        }                                                                                                              \
        if (rule.action == ACT_FAIL) {                                                                                 \
            errno = rule.err ? rule.err : EIO;                                                                         \
            return -1;                                                                                                 \
        }                                                                                                              \
        if (!rule.after) act(&rule);                                                                                   \
        errno = saved;                                                                                                 \
        type result = real;                                                                                            \
        saved = errno;                                                                                                 \
        if (rule.after) act(&rule);                                                                                    \
        errno = saved;                                                                                                 \
        return result;                                                                                                 \
    } while (0)

ssize_t write(int fd, const void *buf, size_t count) {
    if (!real_write) resolve();
    INTERCEPT(ssize_t, CALL_WRITE, fd, NULL, NULL, real_write(fd, buf, count));
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    if (!real_writev) resolve();
    INTERCEPT(ssize_t, CALL_WRITE, fd, NULL, NULL, real_writev(fd, iov, iovcnt));
}

int fsync(int fd) {
    if (!real_fsync) resolve();
    INTERCEPT(int, CALL_FSYNC, fd, NULL, NULL, real_fsync(fd));
}

int fdatasync(int fd) {
    if (!real_fdatasync) resolve();
    INTERCEPT(int, CALL_FDATASYNC, fd, NULL, NULL, real_fdatasync(fd));
}

int rename(const char *from, const char *to) {
    if (!real_rename) resolve();
    INTERCEPT(int, CALL_RENAME, -1, from, to, real_rename(from, to));
}
