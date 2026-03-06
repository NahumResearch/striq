CC      ?= clang
ARCH    := $(shell uname -m)
UNAME_S := $(shell uname -s)
ifeq ($(ARCH),arm64)
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic -Wno-unused-parameter -O2 -march=armv8-a+crc
else
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic -Wno-unused-parameter -O2
endif
INC     = -Ilib -Iinclude
BUILD   = build

# Windows (MSYS2/MinGW) detection
EXE =
ifneq (,$(findstring MINGW,$(UNAME_S)))
EXE = .exe
endif
ifneq (,$(findstring MSYS,$(UNAME_S)))
EXE = .exe
endif

# ── Core library sources ──────────────────────────────────────────────

CORE_SRCS = \
	lib/core/codecs/dod.c \
	lib/core/codecs/pla.c \
	lib/core/codecs/chebyshev.c \
	lib/core/codecs/raw_stats.c \
	lib/core/codecs/decimal.c \
	lib/core/codecs/quant.c \
	lib/core/codecs/residuals.c \
	lib/core/codecs/crc32.c \
	lib/core/codecs/rle_codec.c \
	lib/core/routing/stats.c \
	lib/core/routing/router.c \
	lib/core/format/writer.c \
	lib/core/format/reader.c \
	lib/core/query/algebra.c \
	lib/core/query/engine.c \
	lib/core/encoder.c \
	lib/core/decoder.c

ifeq ($(ARCH),arm64)
CORE_SRCS += lib/platform/simd_neon.c
else
CORE_SRCS += lib/platform/simd_scalar.c
endif

CORE_OBJS = $(patsubst %.c,$(BUILD)/%.o,$(CORE_SRCS))

ADAPTER_SRCS = lib/adapters/file_io.c lib/adapters/c_api.c \
               lib/adapters/file_provider.c \
               lib/adapters/mmap_provider.c \
               lib/adapters/memstore.c \
               lib/adapters/store.c \
               lib/adapters/manifest.c \
               lib/adapters/partition.c
ADAPTER_OBJS = $(BUILD)/file_io.o $(BUILD)/c_api.o $(BUILD)/file_provider.o \
               $(BUILD)/mmap_provider.o \
               $(BUILD)/memstore.o $(BUILD)/store.o \
               $(BUILD)/manifest.o $(BUILD)/partition.o

ALL_OBJS = $(CORE_OBJS) $(ADAPTER_OBJS)

# ── Tests ─────────────────────────────────────────────────────────────

TESTS = test_dod test_pla test_stats test_router \
        test_format test_encoder test_query test_roundtrip \
        test_compression_bench test_query_accuracy test_multicolumn \
        test_time_range test_epsilon_invariant test_integration \
        test_residuals test_memstore test_store test_partition \
        test_chebyshev test_integrity \
        test_rle test_simd test_raw_stats \
        test_decimal test_quant

PREFIX ?= /usr/local

.PHONY: all clean test bench bench_epsilon demos install plots

# ── Default: library + CLI only (zero external deps) ──────────────────

all: $(BUILD)/libstriq.a $(BUILD)/striq$(EXE)

# ── Static library ────────────────────────────────────────────────────

$(BUILD)/libstriq.a: $(ALL_OBJS)
	ar rcs $@ $^
	@echo "Library: $@"

# ── CLI binary ────────────────────────────────────────────────────────

$(BUILD)/striq$(EXE): lib/adapters/cli.c $(BUILD)/libstriq.a
	$(CC) $(CFLAGS) $(INC) $^ -lm -o $@
	@echo "CLI: $@"

# ── Pattern rule for object files ─────────────────────────────────────

$(BUILD)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

# ── Adapter objects (flat names) ──────────────────────────────────────

$(BUILD)/file_io.o: lib/adapters/file_io.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(BUILD)/c_api.o: lib/adapters/c_api.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(BUILD)/file_provider.o: lib/adapters/file_provider.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(BUILD)/mmap_provider.o: lib/adapters/mmap_provider.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(BUILD)/memstore.o: lib/adapters/memstore.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(BUILD)/store.o: lib/adapters/store.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(BUILD)/manifest.o: lib/adapters/manifest.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

$(BUILD)/partition.o: lib/adapters/partition.c
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

# ── Tests ─────────────────────────────────────────────────────────────

define make_test
$(BUILD)/$(1)$(EXE): tests/$(1).c $(BUILD)/libstriq.a
	$(CC) $(CFLAGS) $(INC) $$^ -lm -o $$@
endef

$(foreach t,$(TESTS),$(eval $(call make_test,$(t))))

TEST_BINS = $(addsuffix $(EXE),$(addprefix $(BUILD)/,$(TESTS)))

test: $(TEST_BINS)
	echo ""; echo "=== Running all tests ==="; echo ""
	failed=0; \
	for t in $(TEST_BINS); do \
		echo "--- $$t ---"; \
		$$t || { echo "FAILED: $$t"; failed=1; }; \
		echo ""; \
	done; \
	[ $$failed -eq 0 ] && echo "=== All tests PASSED ===" || { echo "=== SOME TESTS FAILED ==="; exit 1; }

# ── Benchmarks (require system lz4 + zstd for competitor comparison) ──

BREW_PREFIX ?= $(shell brew --prefix 2>/dev/null || echo /usr/local)
BENCH_INC   = -Ibench/codecs -Ibench/harness -I$(BREW_PREFIX)/include
BENCH_LIBS  = -L$(BREW_PREFIX)/lib -llz4 -lzstd -lm

$(BUILD)/bench_main$(EXE): bench/bench_main.c bench/codecs/gorilla.c \
                     bench/harness/csv_loader.c bench/harness/report.c \
                     $(BUILD)/libstriq.a
	$(CC) $(CFLAGS) $(INC) $(BENCH_INC) $^ $(BENCH_LIBS) -o $@

bench: $(BUILD)/bench_main$(EXE)
	@echo ""
	@echo "=== STRIQ Benchmark (vs Gorilla / LZ4 / Zstd) ==="
	$(BUILD)/bench_main$(EXE)

$(BUILD)/bench_epsilon$(EXE): bench/bench_epsilon.c \
                        bench/harness/csv_loader.c \
                        $(BUILD)/libstriq.a
	$(CC) $(CFLAGS) $(INC) -Ibench/harness $^ -lm -o $@

bench_epsilon: $(BUILD)/bench_epsilon$(EXE)
	@echo ""
	@echo "=== STRIQ Epsilon Sweep ==="
	$(BUILD)/bench_epsilon$(EXE)

# ── Demos ─────────────────────────────────────────────────────────────

$(BUILD)/demo_store$(EXE): scripts/demo_store.c $(BUILD)/libstriq.a
	$(CC) $(CFLAGS) $(INC) $^ -lm -o $@

$(BUILD)/demo_partition$(EXE): scripts/demo_partition.c $(BUILD)/libstriq.a
	$(CC) $(CFLAGS) $(INC) $^ -lm -o $@

demos: $(BUILD)/demo_store$(EXE) $(BUILD)/demo_partition$(EXE)

# ── Plots (requires Python 3 + plotly + kaleido) ─────────────────────

plots:
	python3 bench/plot.py

# ── Install ───────────────────────────────────────────────────────────

install: $(BUILD)/libstriq.a $(BUILD)/striq$(EXE)
	install -d $(DESTDIR)$(PREFIX)/lib
	install -d $(DESTDIR)$(PREFIX)/include
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 644 $(BUILD)/libstriq.a $(DESTDIR)$(PREFIX)/lib/
	install -m 644 include/striq.h     $(DESTDIR)$(PREFIX)/include/
	install -m 755 $(BUILD)/striq$(EXE) $(DESTDIR)$(PREFIX)/bin/
	@echo "Installed to $(DESTDIR)$(PREFIX)"

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/striq$(EXE)
	rm -f $(DESTDIR)$(PREFIX)/lib/libstriq.a
	rm -f $(DESTDIR)$(PREFIX)/include/striq.h
	@echo "Uninstalled from $(DESTDIR)$(PREFIX)"

clean:
	rm -rf $(BUILD)
