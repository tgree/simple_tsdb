# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
TESTS := \
	 bitmap_test \
	 database_test \
	 root_test

bitmap_test.OBJ := \
	$(MODULE_TBUILD_DIR)/bitmap_test.o
database_test.OBJ := \
	$(MODULE_TBUILD_DIR)/database_test.o \
	$(PARENT_TBUILD_DIR)/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
root_test.LIB := libcrypto.a
root_test.OBJ := \
	$(MODULE_TBUILD_DIR)/root_test.o \
	$(PARENT_TBUILD_DIR)/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
