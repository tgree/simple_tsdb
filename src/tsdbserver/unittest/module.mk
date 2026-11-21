# Copyright (c) 2026 by Terry Greeniaus.
# All rights reserved.
TESTS := \
	 reflector_test

reflector_test.LIB := libz-ng.a
reflector_test.DYLIB := ssl crypto
reflector_test.OBJ := \
	$(MODULE_TBUILD_DIR)/reflector_test.o \
	$(PARENT_TBUILD_DIR)/reflector.o \
	$(BUILD_TO_DIR)/libtsdb/unittest/fake_db.o \
	$(BUILD_TO_DIR)/libtsdb/delete.o \
	$(BUILD_TO_DIR)/libtsdb/wal.o \
	$(BUILD_TO_DIR)/libtsdb/write.o \
	$(BUILD_TO_DIR)/libtsdb/count.o \
	$(BUILD_TO_DIR)/libtsdb/select_op.o \
	$(BUILD_TO_DIR)/libtsdb/series.o \
	$(BUILD_TO_DIR)/libtsdb/measurement.o \
	$(BUILD_TO_DIR)/libtsdb/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
