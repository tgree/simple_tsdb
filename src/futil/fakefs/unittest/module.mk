# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
TESTS := \
	 snapshot_test \
	 wrap_test

snapshot_test.OBJ := \
	$(MODULE_TBUILD_DIR)/snapshot_test.o \
	$(PARENT_TBUILD_DIR)/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o

wrap_test.OBJ := \
	$(MODULE_TBUILD_DIR)/wrap_test.o \
	$(PARENT_TBUILD_DIR)/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
