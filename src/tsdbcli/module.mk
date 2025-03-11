# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
SUBMODULES :=

tsdbcli.MK  := $(MODULE_MK)
tsdbcli.LIB :=
tsdbcli.OBJ := \
	$(MODULE_BUILD_DIR)/main.o \
	$(BUILD_O_DIR)/floor/kassert.o \
	$(BUILD_O_DIR)/libtsdb/libtsdb.o
