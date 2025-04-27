# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
TESTS := \
	 futil_test \
	 sockaddr_test \
	 tcp_test

futil_test.OBJ := \
	$(MODULE_TBUILD_DIR)/futil_test.o
sockaddr_test.OBJ := \
	$(MODULE_TBUILD_DIR)/sockaddr_test.o
tcp_test.OBJ := \
	$(MODULE_TBUILD_DIR)/tcp_test.o \
	$(BUILD_O_DIR)/floor/kassert.o
