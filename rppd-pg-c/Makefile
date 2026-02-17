# rppd PostgreSQL Extension Makefile
# Uses PGXS (PostgreSQL Extension Building Infrastructure)

# Extension version from rppd-common
RPPD_VERSION := $(shell grep '^version =' ../rppd-common/Cargo.toml | sed 's/version = "//;s/"//')

EXTENSION = rppd
MODULE_big = rppd
DATA = rppd--$(RPPD_VERSION).sql
OBJS = src/rppd.o src/grpc_client.o src/protobuf.o

# Get PostgreSQL major version
PG_VERSION := $(shell pg_config --version | sed 's/PostgreSQL //' | cut -d. -f1)

# Get glibc version with underscores
GLIBC_VERSION := $(shell ldd --version | head -n 1 | awk '{print $$NF}' | sed 's/\./_/g')

# Versioned library name
VERSIONED_SO = rppd$(RPPD_VERSION)-pg$(PG_VERSION)-glibc$(GLIBC_VERSION).so

# Compiler flags
PG_CPPFLAGS = -I$(shell pg_config --includedir) -I./src
PG_CFLAGS = -Wall -Wextra -Wno-unused-parameter -Werror=implicit-function-declaration

# Required libraries: libcurl for HTTP/2, protobuf-c for protocol buffers
SHLIB_LINK = -lcurl -lprotobuf-c

# Use PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Generate protobuf C files from .proto
PROTO_DIR = ../rppd-common/protos
PROTO_OUT = src/proto

proto: $(PROTO_OUT)/rppc.pb-c.c $(PROTO_OUT)/rppg.pb-c.c

$(PROTO_OUT)/rppc.pb-c.c: $(PROTO_DIR)/rppc.proto
	@mkdir -p $(PROTO_OUT)
	protoc-c --c_out=$(PROTO_OUT) --proto_path=$(PROTO_DIR) $<

$(PROTO_OUT)/rppg.pb-c.c: $(PROTO_DIR)/rppg.proto $(PROTO_DIR)/rppc.proto
	@mkdir -p $(PROTO_OUT)
	protoc-c --c_out=$(PROTO_OUT) --proto_path=$(PROTO_DIR) $<

# Update control file version
$(EXTENSION).control: $(EXTENSION).control.in
	sed 's/@RPPD_VERSION@/$(RPPD_VERSION)/g' $< > $@

# Post-build: create versioned copy
all: $(VERSIONED_SO)

$(VERSIONED_SO): rppd.so
	cp $< $@
	@echo "Created versioned library: $(VERSIONED_SO)"

clean-proto:
	rm -rf $(PROTO_OUT)

.PHONY: proto clean-proto

clean-all: clean
	rm -f src/*.o src/*.bc rppd*.so

info:
	@echo "Extension: $(EXTENSION)"
	@echo "Version: $(RPPD_VERSION)"
	@echo "PostgreSQL: $(PG_VERSION)"
	@echo "glibc: $(GLIBC_VERSION)"
	@echo "Output: $(VERSIONED_SO)"
