# Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.

# Rule to generate a single header "root" directory.  This places a symbolic
# link to the module directory inside $(INCLUDE_DIR), allowing you to do:
# 	#include <module/hdr.h>
# Instead of:
# 	#include <path/to/module/hdr.h>
$(INCLUDE_DIR)/%: $(SRC_DIR)/%
	@mkdir -p $(dir $@)
	ln -r -s $^ $@

# Rule to generate all header root directories.
.PHONY: headers
headers: $(HEADERS:$(SRC_DIR)/%=$(INCLUDE_DIR)/%)
	@:

# Rule to generate GIT commit version number.
.PHONY: version
version:
	@:
