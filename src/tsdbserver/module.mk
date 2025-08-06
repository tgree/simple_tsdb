# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
SUBMODULES :=

tsdbserver.MK  := $(MODULE_MK)
tsdbserver.LIB := libtsdb.a libz-ng.a libssl.a libcrypto.a
tsdbserver.OBJ := \
	$(MODULE_BUILD_DIR)/server.o \
	$(BUILD_O_DIR)/floor/kassert.o

tsdbclienttest.MK  := $(MODULE_MK)
tsdbclienttest.LIB := libtsdb.a libz-ng.a libssl.a libcrypto.a
tsdbclienttest.OBJ := \
	$(MODULE_BUILD_DIR)/client_test.o \
	$(MODULE_BUILD_DIR)/client.o \
	$(BUILD_O_DIR)/floor/kassert.o
