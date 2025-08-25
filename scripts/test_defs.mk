# Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.

# Rule to build a native unittest object file from a %.cc file.
$(BUILD_TO_DIR)/%.o: $(SRC_DIR)/%.cc | headers
	@echo Compiling test object $(SRC_DIR)/$*.cc...
	@mkdir -p $(dir $@)
	@$(CXX) \
		-MMD \
		-MP \
		-MF $(BUILD_TO_DIR)/$*.d \
		-c \
		$(TEST_CXXFLAGS) \
		$(SRC_DIR)/$*.cc \
		-o $@

# Rule to generate the result from running a single unittest.  This works by
# executing the test binary and then generating %.tpass files under
# $(TESTS_DIR) if the binary exited cleanly.
$(TEST_RES_DIR)/%.tpass: $(TESTS_DIR)/%
	@echo Running $^...
	@mkdir -p $(TEST_RES_DIR)
	@$^ && touch $@

# Canned recipe to create a test binary.
# Each test, T, should provide the following fields in their module.mk file:
# 	T.LIB - list of .a dependencies
# 	T.OBJ - list of .o dependencies
BUILD_TEST = mkdir -p $(TESTS_DIR) && $(TEST_CXX) $(TEST_CXXFLAGS) -o $@
define define_test
-include $$($(1).OBJ:.o=.d)
$$(TESTS_DIR)/$(1): $$($(1).OBJ) $$($(1).LIB:%.a=$$(LIB_DIR)/%.a) $$(LTMOCK) $$($(1).MK)
	@echo Building $$@
	@$$(BUILD_TEST) \
		$$($(1).OBJ) \
		$$($(1).LIB:%.a=$$(LIB_DIR)/%.a) \
		$$(LTMOCK)
endef

define define_all_tests
	$(eval $(foreach T,$(ALL_TESTS),$(eval $(call define_test,$(T)))))
endef
