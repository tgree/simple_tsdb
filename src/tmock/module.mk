# Copyright (c) 2018-2019 Terry Greeniaus.
# All rights reserved.
SUBMODULES := unittest

TMOCK_SRCS := $(sort $(wildcard $(MODULE_SRC_DIR)/*.cc))
TMOCK_OBJ := $(patsubst $(MODULE_SRC_DIR)/%.cc,$(MODULE_TBUILD_DIR)/%.o,$(TMOCK_SRCS))

-include $(TMOCK_OBJ:.o=.d)

LTMOCK := $(MODULE_TBUILD_DIR)/tmock.a
$(LTMOCK): $(TMOCK_OBJ) $(MODULE_MK)
	@echo Archiving $@...
	@$(AR) $(ARFLAGS) $@ $(TMOCK_OBJ)
