# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
SUBMODULES :=

tsdbwaltest.MK  := $(MODULE_MK)
tsdbwaltest.LIB := libtsdb.a libz-ng.a libssl.a libcrypto.a
tsdbwaltest.OBJ := \
	$(MODULE_BUILD_DIR)/main.o \
	$(BUILD_O_DIR)/floor/kassert.o
