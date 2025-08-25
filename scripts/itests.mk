# Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.

# Rule to execute all tests by generating all their %.tpass files, nuking the
# %.tpass first if 'make test' was entered.
ifeq ($(MAKECMDGOALS),itest)
$(shell rm -rf $(ITEST_RES_DIR))
endif
.PHONY: itest
itest: $(ALL_ITESTS:%=$(ITEST_RES_DIR)/%.tpass)
	$(info All integration tests passed.)
