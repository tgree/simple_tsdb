# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
SUBMODULES :=

tsdbcli.MK  := $(MODULE_MK)
tsdbcli.LIB := libtsdb.a libfutil.a libz-ng.a libssl.a libcrypto.a
tsdbcli.OBJ := \
	$(MODULE_BUILD_DIR)/main.o \
	$(BUILD_O_DIR)/floor/kassert.o
