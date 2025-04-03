# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
SUBMODULES :=

tsdbtest.MK  := $(MODULE_MK)
tsdbtest.LIB := libtsdb.a libz-ng.a libssl.a libcrypto.a
tsdbtest.OBJ := \
	$(MODULE_BUILD_DIR)/main.o \
	$(BUILD_O_DIR)/floor/kassert.o
