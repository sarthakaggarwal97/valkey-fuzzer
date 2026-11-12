/* The constants tests/fixtures/config_space/config.c names, as Valkey's src/server.h and src/config.h define them. */
#define CONFIG_DEFAULT_HZ 10 /* Time interrupt calls/sec. */
#define IO_THREADS_MAX_NUM 256
#define MAXMEMORY_FLAG_LRU (1 << 0)
#define MAXMEMORY_FLAG_ALLKEYS (1 << 2)
#define MAXMEMORY_VOLATILE_LRU ((0 << 8) | MAXMEMORY_FLAG_LRU)
#define MAXMEMORY_ALLKEYS_LRU ((4 << 8) | MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_NO_EVICTION (7 << 8)
#define SELECTOR_FLAG_ALLCHANNELS (1 << 3)
