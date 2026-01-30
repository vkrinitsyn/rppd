# rppd PostgreSQL Extension Makefile
# Uses PGXS (PostgreSQL Extension Building Infrastructure)

EXTENSION = rppd
MODULE_big = rppd
DATA = rppd--0.2.2.sql
OBJS = src/rppd.o src/grpc_client.o src/protobuf.o

# Extension version
RPPD_VERSION = 0.2.2

# Compiler flags
PG_CPPFLAGS = -I$(shell pg_config --includedir) -I./src
PG_CFLAGS = -Wall -Wextra -Werror=implicit-function-declaration

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

clean-proto:
	rm -rf $(PROTO_OUT)

.PHONY: proto clean-proto
