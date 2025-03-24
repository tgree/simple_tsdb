# Copyright (c) 2025 by Terry Greeniaus.  All rights reserved.

$(LIB_DIR)/libz-ng.a: $(THIRD_PARTY)/zlib-ng/libz-ng.a
	@echo Symlinking $@...
	@mkdir -p $(LIB_DIR)
	@ln -s ../../$(THIRD_PARTY)/zlib-ng/libz-ng.a $@
