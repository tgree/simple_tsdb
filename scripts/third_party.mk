# Copyright (c) 2025 by Terry Greeniaus.  All rights reserved.

$(LIB_DIR)/libz-ng.a: $(THIRD_PARTY)/zlib-ng/libz-ng.a
	@echo Symlinking $@...
	@mkdir -p $(LIB_DIR)
	@ln -s ../../$(THIRD_PARTY)/zlib-ng/libz-ng.a $@

$(LIB_DIR)/libeditline.a: $(THIRD_PARTY)/editline/src/.libs/libeditline.a
	@echo Symlinking $@...
	@mkdir -p $(LIB_DIR)
	@ln -s ../../$(THIRD_PARTY)/editline/src/.libs/libeditline.a $@

ifeq ($(UNAME_S), Darwin)
$(LIB_DIR)/libssl.a: /opt/homebrew/opt/openssl/lib/libssl.a
	@echo Symlinking $@...
	@mkdir -p $(LIB_DIR)
	@ln -s $< $@

$(LIB_DIR)/libcrypto.a: /opt/homebrew/opt/openssl/lib/libcrypto.a
	@echo Symlinking $@...
	@mkdir -p $(LIB_DIR)
	@ln -s $< $@
endif

ifeq ($(UNAME_S), Linux)
$(LIB_DIR)/libssl.a: /usr/lib/x86_64-linux-gnu/libssl.a
	@echo Symlinking $@...
	@mkdir -p $(LIB_DIR)
	@ln -s $< $@

$(LIB_DIR)/libcrypto.a: /usr/lib/x86_64-linux-gnu/libcrypto.a
	@echo Symlinking $@...
	@mkdir -p $(LIB_DIR)
	@ln -s $< $@
endif
