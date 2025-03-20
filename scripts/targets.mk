# Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.

# Canned recipe to define a rule for linking a .elf target.
# Parameters:
# 	$(1) - the name of the target
define define_target_rule
-include $$($(1).OBJ:%.o=%.d)
$$(BIN_DIR)/$(1).map $$(BIN_DIR)/$(1): $$($(1).LIB:%.a=$$(LIB_DIR)/%.a) $$($(1).OBJ) $$($(1).MK) \
				       scripts/targets.mk
	@echo Linking $$@...
	@mkdir -p $$(BIN_DIR)
	$$(GCC_CXX) \
		-g \
		-o $$@ \
		$$($(1).LIB:%.a=$$(LIB_DIR)/%.a) \
		$$($(1).OBJ)
endef

# Canned recipe to define all link rules for a list of targets.
# Parameters:
# 	$(1) - the list of targets
define define_target_rules
	$(foreach T,$(1),$(eval $(call define_target_rule,$(T))))
endef
