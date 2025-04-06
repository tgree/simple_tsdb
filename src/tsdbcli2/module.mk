# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
SUBMODULES :=

tsdbcli2.MK  := $(MODULE_MK)
tsdbcli2.LIB := libtsdb.a libfutil.a libz-ng.a libeditline.a libssl.a libcrypto.a
tsdbcli2.OBJ := \
	$(MODULE_BUILD_DIR)/parse_types.o \
	$(MODULE_BUILD_DIR)/print_op_results.o \
	$(MODULE_BUILD_DIR)/main.o \
	$(BUILD_O_DIR)/floor/kassert.o
