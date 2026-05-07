# DataFlow OS — build system
CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic \
           -D_POSIX_C_SOURCE=200809L -D_GNU_SOURCE \
           -Ilib -Ilib/core -Ilib/net -Ilib/storage \
           -Ilib/sql_parser -Ilib/qengine -Ilib/scheduler \
           -Ilib/observ -Ilib/connector -Ilib/index
LDFLAGS = -lpthread -lm -ldl -lsqlite3 -lcurl -lcrypto -lssl

# Release flags
REL_FLAGS = -O2 -DNDEBUG -flto
# Debug flags
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
            lib/core/threadpool.c lib/core/json.c lib/auth/auth.c
NET_SRCS  = lib/net/http.c lib/net/tls.c
STOR_SRCS = lib/storage/storage.c lib/index/btree.c
SQL_SRCS  = lib/sql_parser/sql.c
QE_SRCS   = lib/qengine/qengine.c
SCHED_SRCS= lib/scheduler/scheduler.c
OBS_SRCS  = lib/observ/observ.c
CONN_SRCS = lib/connector/connector.c

ALL_LIB_SRCS = $(CORE_SRCS) $(NET_SRCS) $(STOR_SRCS) $(SQL_SRCS) \
               $(QE_SRCS) $(SCHED_SRCS) $(OBS_SRCS) $(CONN_SRCS)

GW_SRCS   = src/gateway/main.c src/gateway/api.c

# convert .c to .o in OUTDIR
ALL_OBJS  = $(patsubst %.c,$(OUTDIR)/%.o,$(ALL_LIB_SRCS) $(GW_SRCS))

GATEWAY        = $(BINDIR)/dfo_gateway
CSV_PLUGIN     = $(LIBDIR)/csv_connector.so
PG_PLUGIN      = $(LIBDIR)/pg_connector.so
PARQUET_PLUGIN = $(LIBDIR)/parquet_connector.so
JSONHTTP_PLUGIN= $(LIBDIR)/json_http_connector.so

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

.PHONY: all clean run test dirs release debug

ifeq ($(HAS_PQ),yes)
all: dirs $(GATEWAY) $(CSV_PLUGIN) $(PG_PLUGIN) $(PARQUET_PLUGIN) $(JSONHTTP_PLUGIN)
else
all: dirs $(GATEWAY) $(CSV_PLUGIN) $(PARQUET_PLUGIN) $(JSONHTTP_PLUGIN)
endif

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
	           $(OUTDIR)/lib/auth $(OUTDIR)/src/gateway

# compile rule
$(OUTDIR)/%.o: %.c
	@echo "  CC  $<"
	@$(CC) $(CFLAGS) -c $< -o $@

# gateway binary
$(GATEWAY): $(ALL_OBJS)
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

# ── Tests ──
TEST_SRCS = $(wildcard tests/unit/*.c)
TEST_BINS = $(patsubst tests/unit/%.c,$(BINDIR)/test_%,$(TEST_SRCS))

$(BINDIR)/test_%: tests/unit/%.c $(filter-out $(OUTDIR)/src/gateway/main.o, $(ALL_OBJS))
	@echo "  TC  $<"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test: dirs $(TEST_BINS)
	@for t in $(TEST_BINS); do echo "\n=== $$t ==="; $$t; done

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
