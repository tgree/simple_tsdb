# Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.

# Rule to execute all tests by generating all their %.tpass files, nuking the
# %.tpass first if 'make test' was entered.
ifeq ($(MAKECMDGOALS),test)
$(shell rm -rf $(TEST_RES_DIR))
endif
.PHONY: test
test: $(ALL_TESTS:%=$(TEST_RES_DIR)/%.tpass)
	$(info All unittests passed.)
