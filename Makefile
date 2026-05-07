# DataFlow OS — build system
CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic \
           -D_POSIX_C_SOURCE=200809L -D_GNU_SOURCE \
           -Ilib -Ilib/core -Ilib/net -Ilib/storage \
           -Ilib/sql_parser -Ilib/qengine -Ilib/scheduler \
           -Ilib/observ -Ilib/connector -Ilib/index
LDFLAGS = -lpthread -lm -ldl -lsqlite3 -lcurl -lcrypto -lssl

# Release flags (compression enabled)
REL_FLAGS = -O2 -DNDEBUG -flto -DENABLE_COMPRESSION=1
# Debug flags (compression disabled — упрощает отладку WAL-формата)
DBG_FLAGS = -O0 -g3 -fsanitize=address,undefined -DDEBUG

BUILD ?= debug
ifeq ($(BUILD),release)
  CFLAGS  += $(REL_FLAGS)
else
  CFLAGS  += $(DBG_FLAGS)
  LDFLAGS += -fsanitize=address,undefined
endif

OUTDIR  = build/$(BUILD)
BINDIR  = $(OUTDIR)/bin
LIBDIR  = $(OUTDIR)/lib

# ── Core library objects ──
CORE_SRCS = lib/core/arena.c lib/core/log.c lib/core/hashmap.c \
            lib/core/threadpool.c lib/core/json.c lib/auth/auth.c \
            lib/auth/rbac.c lib/auth/audit.c
NET_SRCS  = lib/net/http.c lib/net/tls.c
STOR_SRCS = lib/storage/storage.c lib/storage/compress.c lib/storage/txn.c lib/index/btree.c
SQL_SRCS  = lib/sql_parser/sql.c
QE_SRCS   = lib/qengine/qengine.c
SCHED_SRCS= lib/scheduler/scheduler.c
OBS_SRCS  = lib/observ/observ.c
CONN_SRCS = lib/connector/connector.c
MV_SRCS   = lib/matview/matview.c
CL_SRCS   = lib/cluster/proto.c lib/cluster/storage_client.c lib/cluster/replicator.c

ALL_LIB_SRCS = $(CORE_SRCS) $(NET_SRCS) $(STOR_SRCS) $(SQL_SRCS) \
               $(QE_SRCS) $(SCHED_SRCS) $(OBS_SRCS) $(CONN_SRCS) \
               $(MV_SRCS) $(CL_SRCS)

GW_SRCS   = src/gateway/main.c src/gateway/api.c
SN_SRCS   = src/storage_node/main.c

# convert .c to .o in OUTDIR
ALL_OBJS  = $(patsubst %.c,$(OUTDIR)/%.o,$(ALL_LIB_SRCS) $(GW_SRCS))
LIB_OBJS  = $(patsubst %.c,$(OUTDIR)/%.o,$(ALL_LIB_SRCS))

GATEWAY        = $(BINDIR)/dfo_gateway
STORAGE_NODE   = $(BINDIR)/dfo_storage
CSV_PLUGIN     = $(LIBDIR)/csv_connector.so
PG_PLUGIN      = $(LIBDIR)/pg_connector.so
PARQUET_PLUGIN = $(LIBDIR)/parquet_connector.so
JSONHTTP_PLUGIN= $(LIBDIR)/json_http_connector.so
S3_PLUGIN      = $(LIBDIR)/s3_connector.so
KAFKA_PLUGIN   = $(LIBDIR)/kafka_connector.so

# Detect librdkafka
_RDKAFKA_LOCAL := $(shell test -d /usr/local/opt/librdkafka/include && echo /usr/local/opt/librdkafka)
_RDKAFKA_BREW  := $(shell test -d /opt/homebrew/opt/librdkafka/include && echo /opt/homebrew/opt/librdkafka)
RDKAFKA_PREFIX := $(or $(_RDKAFKA_LOCAL),$(_RDKAFKA_BREW))
ifneq ($(RDKAFKA_PREFIX),)
  KAFKACFLAGS  = -I$(RDKAFKA_PREFIX)/include
  KAFKALDFLAGS = -L$(RDKAFKA_PREFIX)/lib -lrdkafka
  HAS_RDKAFKA  = yes
else
  KAFKACFLAGS  := $(shell pkg-config --cflags rdkafka 2>/dev/null)
  KAFKALDFLAGS := $(shell pkg-config --libs rdkafka 2>/dev/null)
  HAS_RDKAFKA  := $(if $(KAFKALDFLAGS),yes,no)
endif

# Detect libpq — homebrew keg-only or pkg-config
_LIBPQ_LOCAL := $(shell test -d /usr/local/opt/libpq/include && echo /usr/local/opt/libpq)
_LIBPQ_BREW  := $(shell test -d /opt/homebrew/opt/libpq/include && echo /opt/homebrew/opt/libpq)
LIBPQ_PREFIX := $(or $(_LIBPQ_LOCAL),$(_LIBPQ_BREW))
ifneq ($(LIBPQ_PREFIX),)
  PGCFLAGS  = -I$(LIBPQ_PREFIX)/include
  PGLDFLAGS = -L$(LIBPQ_PREFIX)/lib -lpq
  HAS_PQ    = yes
else
  PGCFLAGS  := $(shell pkg-config --cflags libpq 2>/dev/null)
  PGLDFLAGS := $(shell pkg-config --libs libpq 2>/dev/null)
  HAS_PQ    := $(if $(PGLDFLAGS),yes,no)
endif

.PHONY: all clean run test dirs release debug \
        test-integration test-sql test-all bench

# Base targets always built
_ALL_TARGETS = dirs $(GATEWAY) $(STORAGE_NODE) $(CSV_PLUGIN) $(PARQUET_PLUGIN) $(JSONHTTP_PLUGIN) $(S3_PLUGIN)
ifeq ($(HAS_PQ),yes)
  _ALL_TARGETS += $(PG_PLUGIN)
endif
ifeq ($(HAS_RDKAFKA),yes)
  _ALL_TARGETS += $(KAFKA_PLUGIN)
endif

all: $(_ALL_TARGETS)

release:
	$(MAKE) BUILD=release all

debug:
	$(MAKE) BUILD=debug all

dirs:
	@mkdir -p $(BINDIR) $(LIBDIR)
	@mkdir -p $(OUTDIR)/lib/core $(OUTDIR)/lib/net $(OUTDIR)/lib/storage \
	           $(OUTDIR)/lib/sql_parser $(OUTDIR)/lib/qengine \
	           $(OUTDIR)/lib/scheduler $(OUTDIR)/lib/observ \
	           $(OUTDIR)/lib/connector $(OUTDIR)/lib/index \
	           $(OUTDIR)/lib/auth $(OUTDIR)/lib/matview $(OUTDIR)/lib/cluster \
	           $(OUTDIR)/src/gateway $(OUTDIR)/src/storage_node \
	           $(OUTDIR)/lib/connector/plugins/s3 \
	           $(OUTDIR)/lib/connector/plugins/kafka

# compile rule
$(OUTDIR)/%.o: %.c
	@echo "  CC  $<"
	@$(CC) $(CFLAGS) -c $< -o $@

# gateway binary
$(GATEWAY): $(ALL_OBJS)
	@echo "  LD  $@"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

# storage node binary (cluster replica)
SN_OBJS = $(patsubst %.c,$(OUTDIR)/%.o,$(ALL_LIB_SRCS) $(SN_SRCS))
$(STORAGE_NODE): $(SN_OBJS)
	@echo "  LD  $@"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

# CSV connector shared library
$(CSV_PLUGIN): lib/connector/plugins/csv/csv_connector.c \
               $(OUTDIR)/lib/core/arena.o \
               $(OUTDIR)/lib/storage/storage.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# PostgreSQL connector shared library (requires libpq)
$(PG_PLUGIN): lib/connector/plugins/pg/pg_connector.c \
              $(OUTDIR)/lib/core/arena.o \
              $(OUTDIR)/lib/storage/storage.o \
              $(OUTDIR)/lib/core/log.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) $(PGCFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) $(PGLDFLAGS) \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# Parquet connector (requires zlib)
$(PARQUET_PLUGIN): lib/connector/plugins/parquet/parquet_connector.c \
                   $(OUTDIR)/lib/core/arena.o \
                   $(OUTDIR)/lib/storage/storage.o \
                   $(OUTDIR)/lib/core/log.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) -lz \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# JSON HTTP connector (requires libcurl)
$(JSONHTTP_PLUGIN): lib/connector/plugins/json_http/json_http_connector.c \
                    $(OUTDIR)/lib/core/arena.o \
                    $(OUTDIR)/lib/storage/storage.o \
                    $(OUTDIR)/lib/core/log.o \
                    $(OUTDIR)/lib/core/json.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) -lcurl \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# S3 / MinIO connector (requires libcurl + libcrypto)
$(S3_PLUGIN): lib/connector/plugins/s3/s3_connector.c \
              lib/connector/plugins/s3/aws_sig4.c \
              $(OUTDIR)/lib/core/arena.o \
              $(OUTDIR)/lib/storage/storage.o \
              $(OUTDIR)/lib/core/log.o \
              $(OUTDIR)/lib/core/json.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) -lcurl -lcrypto \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# Kafka connector (requires librdkafka)
$(KAFKA_PLUGIN): lib/connector/plugins/kafka/kafka_connector.c \
                 $(OUTDIR)/lib/core/arena.o \
                 $(OUTDIR)/lib/storage/storage.o \
                 $(OUTDIR)/lib/core/log.o \
                 $(OUTDIR)/lib/core/json.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) $(KAFKACFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) $(KAFKALDFLAGS) \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# ── Unit tests ──
TEST_SRCS = $(wildcard tests/unit/*.c)
TEST_BINS = $(patsubst tests/unit/%.c,$(BINDIR)/test_%,$(TEST_SRCS))

$(BINDIR)/test_%: tests/unit/%.c $(LIB_OBJS)
	@echo "  TC  $<"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test: dirs $(TEST_BINS)
	@for t in $(TEST_BINS); do echo "\n=== $$t ==="; $$t; done

# ── SQL unit tests ──
SQL_TEST_SRCS = $(wildcard tests/sql/*.c)
SQL_TEST_BINS = $(patsubst tests/sql/%.c,$(BINDIR)/sql_%,$(SQL_TEST_SRCS))

$(BINDIR)/sql_%: tests/sql/%.c $(LIB_OBJS)
	@echo "  TC  $<"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lm

test-sql: dirs $(SQL_TEST_BINS)
	@for t in $(SQL_TEST_BINS); do echo "\n=== $$t ==="; $$t; done

# ── Integration tests (require running gateway) ──
ITEST_SRCS = $(wildcard tests/integration/test_*.c)
ITEST_BINS = $(patsubst tests/integration/%.c,$(BINDIR)/itest_%,$(ITEST_SRCS))

$(BINDIR)/itest_%: tests/integration/%.c
	@echo "  IC  $<"
	@$(CC) -std=c11 -Wall -Ilib -Ilib/core -Ilib/net -Ilib/storage \
	    -Ilib/sql_parser -Ilib/qengine -Ilib/scheduler -Ilib/observ \
	    -Ilib/connector -Ilib/index \
	    $< -o $@ -lcurl -lpthread -lm

test-integration: dirs all $(ITEST_BINS)
	@for t in $(ITEST_BINS); do \
	    echo "\n=== $$t ==="; \
	    $$t $(GATEWAY); \
	done

# ── Load benchmarks ──
BENCH_SRCS = $(wildcard tests/load/*.c)
BENCH_BINS = $(patsubst tests/load/%.c,$(BINDIR)/bench_%,$(BENCH_SRCS))

$(BINDIR)/bench_%: tests/load/%.c
	@echo "  BC  $<"
	@$(CC) -std=c11 -Wall -Ilib -Ilib/core -Ilib/net -Ilib/storage \
	    -Ilib/sql_parser -Ilib/qengine -Ilib/scheduler -Ilib/observ \
	    -Ilib/connector -Ilib/index \
	    $< -o $@ -lcurl -lpthread -lm

bench: dirs all $(BENCH_BINS)
	@for b in $(BENCH_BINS); do \
	    echo "\n=== $$b ==="; \
	    $$b $(GATEWAY); \
	done

# ── All tests combined ──
test-all: test test-sql test-integration

# ── Run ──
run: all
	@mkdir -p data
	./$(GATEWAY) -p 8080

clean:
	rm -rf build/

# ── Install UI (copy to served dir) ──
install-ui:
	@mkdir -p $(BINDIR)/ui
	cp ui/index.html ui/style.css ui/app.js $(BINDIR)/ui/
	@echo "UI installed to $(BINDIR)/ui/"

# dependency tracking
-include $(ALL_OBJS:.o=.d)
$(OUTDIR)/%.o: %.c
	@$(CC) $(CFLAGS) -MMD -MP -c $< -o $@
