# Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.

# Rule to build a native integration ttest object file from a %.cc file.
$(BUILD_ITO_DIR)/%.o: $(SRC_DIR)/%.cc | headers
	@echo Compiling integration test object $(SRC_DIR)/$*.cc...
	@mkdir -p $(dir $@)
	@$(CXX) \
		-MMD \
		-MP \
		-MF $(BUILD_ITO_DIR)/$*.d \
		-c \
		$(ITEST_CXXFLAGS) \
		$(SRC_DIR)/$*.cc \
		-o $@

# Rule to generate the result from running a single integration test.  This
# works by executing the test binary and then generating %.tpass files under
# $(ITESTS_DIR) if the binary exited cleanly.
$(ITEST_RES_DIR)/%.tpass: $(ITESTS_DIR)/%
	@echo Running $^...
	@mkdir -p $(ITEST_RES_DIR)
	@$^ && touch $@

# Canned recipe to create an integration test binary.
# Each test, T, should provide the following fields in their module.mk file:
# 	T.LIB - list of .a dependencies
# 	T.OBJ - list of .o dependencies
BUILD_ITEST = mkdir -p $(ITESTS_DIR) && $(ITEST_CXX) $(ITEST_CXXFLAGS) $(ITEST_LDFLAGS) -o $@
define define_itest
-include $$($(1).OBJ:.o=.d)
$$(ITESTS_DIR)/$(1): $$($(1).OBJ) $$($(1).LIB:%.a=$$(LIB_DIR)/%.a) $$($(1).MK)
	@echo Building $$@
	@$$(BUILD_ITEST) \
		$$($(1).OBJ) \
		$$($(1).LIB:%.a=$$(LIB_DIR)/%.a)
endef

define define_all_integration_tests
	$(eval $(foreach T,$(ALL_ITESTS),$(eval $(call define_itest,$(T)))))
endef
