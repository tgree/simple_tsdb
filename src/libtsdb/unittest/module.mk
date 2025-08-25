# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
TESTS := \
	 bitmap_test \
	 database_test \
	 measurement_test \
	 root_test \
	 series_test \
	 wal_test \
	 write_test

bitmap_test.OBJ := \
	$(MODULE_TBUILD_DIR)/bitmap_test.o
database_test.OBJ := \
	$(MODULE_TBUILD_DIR)/database_test.o \
	$(PARENT_TBUILD_DIR)/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
measurement_test.OBJ := \
	$(MODULE_TBUILD_DIR)/measurement_test.o \
	$(PARENT_TBUILD_DIR)/measurement.o \
	$(PARENT_TBUILD_DIR)/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
root_test.LIB := libcrypto.a
root_test.OBJ := \
	$(MODULE_TBUILD_DIR)/root_test.o \
	$(PARENT_TBUILD_DIR)/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
series_test.OBJ := \
	$(MODULE_TBUILD_DIR)/series_test.o \
	$(PARENT_TBUILD_DIR)/series.o \
	$(PARENT_TBUILD_DIR)/measurement.o \
	$(PARENT_TBUILD_DIR)/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
wal_test.LIB := libz-ng.a
wal_test.OBJ := \
	$(MODULE_TBUILD_DIR)/wal_test.o \
	$(MODULE_TBUILD_DIR)/fake_db.o \
	$(PARENT_TBUILD_DIR)/wal.o \
	$(PARENT_TBUILD_DIR)/write.o \
	$(PARENT_TBUILD_DIR)/count.o \
	$(PARENT_TBUILD_DIR)/select_op.o \
	$(PARENT_TBUILD_DIR)/series.o \
	$(PARENT_TBUILD_DIR)/measurement.o \
	$(PARENT_TBUILD_DIR)/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
write_test.LIB := libz-ng.a
write_test.OBJ := \
	$(MODULE_TBUILD_DIR)/write_test.o \
	$(PARENT_TBUILD_DIR)/write.o \
	$(PARENT_TBUILD_DIR)/count.o \
	$(PARENT_TBUILD_DIR)/select_op.o \
	$(PARENT_TBUILD_DIR)/series.o \
	$(PARENT_TBUILD_DIR)/measurement.o \
	$(PARENT_TBUILD_DIR)/root.o \
	$(BUILD_TO_DIR)/futil/fakefs/fakefs.o \
	$(BUILD_O_DIR)/floor/kassert.o
