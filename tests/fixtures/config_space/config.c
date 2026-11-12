/* A config table in the form of Valkey's src/config.c (entries taken from it), for tests/test_config_space.py. */
#include "server.h"

#define LOADBUF_SIZE 256

configEnum maxmemory_policy_enum[] = {
    {"volatile-lru", MAXMEMORY_VOLATILE_LRU},
    {"allkeys-lru", MAXMEMORY_ALLKEYS_LRU},
    {"noeviction", MAXMEMORY_NO_EVICTION},
    {NULL, 0}};

configEnum acl_pubsub_default_enum[] = {
    {"allchannels", SELECTOR_FLAG_ALLCHANNELS},
    {"resetchannels", 0},
    {NULL, 0}};

configEnum forkless_policy_enum[] = {{"require-fork", FORKLESS_POLICY_REQUIRE_FORK},
                                     {"prefer-forkless", FORKLESS_POLICY_PREFER_FORKLESS},
                                     {NULL, 0}};

configEnum shutdown_on_sig_enum[] = {{"default", 0}, {"save", SHUTDOWN_SAVE}, {"nosave", SHUTDOWN_NOSAVE}, {NULL, 0}};

#define createBoolConfig(name, alias, flags, config_addr, default, is_valid, apply) \
    { embedCommonConfig(name, alias, flags) }

standardConfig static_configs[] = {
    /* Bool configs */
    createBoolConfig("rdbchecksum", NULL, IMMUTABLE_CONFIG, server.rdb_checksum, 1, NULL, NULL),
    createBoolConfig("lazyfree-lazy-eviction", NULL, DEBUG_CONFIG | MODIFIABLE_CONFIG, server.lazyfree_lazy_eviction, 1, NULL, NULL),
    createBoolConfig("io-threads-always-active", NULL, MODIFIABLE_CONFIG | HIDDEN_CONFIG, server.io_threads_always_active, 0, NULL, NULL), /* comma, in a comment */
    createBoolConfig("lua-enable-insecure-api", "lua-enable-deprecated-api", MODIFIABLE_CONFIG | HIDDEN_CONFIG | PROTECTED_CONFIG, server.lua_enable_insecure_api, 0, NULL, NULL),
    createBoolConfig("forkless-infrastructure-enabled", NULL, IMMUTABLE_CONFIG, server.forkless_infrastructure_enabled, 0, NULL, NULL),
#ifdef LOG_REQ_RES
    createBoolConfig("req-res-logfile-on", NULL, IMMUTABLE_CONFIG | HIDDEN_CONFIG, server.req_res_on, 0, NULL, NULL),
#endif

    /* String configs */
    createStringConfig("dbfilename", NULL, MODIFIABLE_CONFIG | PROTECTED_CONFIG, ALLOW_EMPTY_STRING, server.rdb_filename, "dump.rdb", isValidDBfilename, NULL),

    /* Enum Configs */
    createEnumConfig("maxmemory-policy", NULL, MODIFIABLE_CONFIG, maxmemory_policy_enum, server.maxmemory_policy, MAXMEMORY_NO_EVICTION, NULL, NULL),
    createEnumConfig("acl-pubsub-default", NULL, MODIFIABLE_CONFIG, acl_pubsub_default_enum, server.acl_pubsub_default, 0, NULL, NULL),
    createEnumConfig("forkless-policy", NULL, MODIFIABLE_CONFIG, forkless_policy_enum, server.forkless_policy, FORKLESS_POLICY_REQUIRE_FORK, isValidForklessPolicy, updateForklessPolicy),
    createEnumConfig("shutdown-on-sigterm", NULL, MODIFIABLE_CONFIG | MULTI_ARG_CONFIG, shutdown_on_sig_enum, server.shutdown_on_sigterm, 0, isValidShutdownOnSigFlags, NULL),

    /* Integer configs */
    createIntConfig("io-threads", NULL, DEBUG_CONFIG | IMMUTABLE_CONFIG, 1, IO_THREADS_MAX_NUM, server.io_threads_num, 1, INTEGER_CONFIG, NULL, updateIOThreads),
    createIntConfig("hz", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, server.config_hz, CONFIG_DEFAULT_HZ, INTEGER_CONFIG, NULL, updateHZ),
    createIntConfig("list-max-listpack-size", "list-max-ziplist-size", MODIFIABLE_CONFIG, INT_MIN, INT_MAX, server.list_max_listpack_size, -2, INTEGER_CONFIG, NULL, NULL),
    createUIntConfig("unixsocketperm", NULL, IMMUTABLE_CONFIG, 0, 0777, server.unixsocketperm, 0, OCTAL_CONFIG, NULL, NULL),
    createLongLongConfig("commandlog-request-larger-than", NULL, MODIFIABLE_CONFIG, -1, LLONG_MAX, server.commandlog[COMMANDLOG_TYPE_LARGE_REQUEST].threshold, 1024 * 1024, MEMORY_CONFIG | SIGNED_MEMORY_CONFIG, NULL, NULL),
    createULongLongConfig("maxmemory", NULL, MODIFIABLE_CONFIG, 0, ULLONG_MAX, server.maxmemory, 0, MEMORY_CONFIG, NULL, updateMaxmemory),
    createSSizeTConfig("maxmemory-clients", NULL, MODIFIABLE_CONFIG, -100, SSIZE_MAX, server.maxmemory_clients, 0, MEMORY_CONFIG | PERCENT_CONFIG, NULL, applyClientMaxMemoryUsage),
    createTimeTConfig("repl-backlog-ttl", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, server.repl_backlog_time_limit, 60 * 60, INTEGER_CONFIG, NULL, NULL), /* Default: 1 hour */
    createIntConfig("lfu-log-factor", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, server.lfu_log_factor, UNKNOWN_CONSTANT, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("watchdog-period", NULL, MODIFIABLE_CONFIG | HIDDEN_CONFIG, 0, NOT_A_CONSTANT(1), server.watchdog_period, 0, INTEGER_CONFIG, NULL, updateWatchdogPeriod),

    /* Special configs */
    createSpecialConfig("save", NULL, MODIFIABLE_CONFIG | MULTI_ARG_CONFIG, setConfigSaveOption, getConfigSaveOption, rewriteConfigSaveOption, NULL),

    /* NULL Terminator, this is dropped when we convert to the runtime array. */
    {NULL},
};
