# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
TESTS := \
	 accept_test \
	 client_socket_test \
	 futil_test \
	 sockaddr_test \
	 ssl_test

ITESTS := \
	 fd_name_test \
	 tcp_test

accept_test.OBJ := \
	$(MODULE_TBUILD_DIR)/accept_test.o \
	$(PARENT_TBUILD_DIR)/mock/mock_futil.o \
	$(PARENT_TBUILD_DIR)/mock/mock_net.o
client_socket_test.OBJ := \
	$(MODULE_TBUILD_DIR)/client_socket_test.o \
	$(PARENT_TBUILD_DIR)/mock/mock_futil.o \
	$(PARENT_TBUILD_DIR)/mock/mock_net.o
fd_name_test.OBJ := \
	$(MODULE_ITBUILD_DIR)/fd_name_test.o \
	$(BUILD_O_DIR)/floor/kassert.o
futil_test.OBJ := \
	$(MODULE_TBUILD_DIR)/futil_test.o
sockaddr_test.OBJ := \
	$(MODULE_TBUILD_DIR)/sockaddr_test.o
ssl_test.OBJ := \
	$(MODULE_TBUILD_DIR)/ssl_test.o \
	$(PARENT_TBUILD_DIR)/mock/mock_ssl.o \
	$(BUILD_O_DIR)/floor/kassert.o
tcp_test.OBJ := \
	$(MODULE_ITBUILD_DIR)/tcp_test.o \
	$(BUILD_O_DIR)/floor/kassert.o
