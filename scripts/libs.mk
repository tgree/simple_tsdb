# Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.

# Canned recipe to define a rule for generating an M0+-compatible library.
# Parameters:
# 	$(1) - the base name of the library
#
# The parameter should have a .OBJ extension that defines the generic name of
# each included object file.  I.e. "floor.o".
define define_lib
-include $$($(1).OBJ:.o=.d)
$$(LIB_DIR)/$(1).a: $(1).OBJ $$(MODULE_MK)
	@echo Archiving $$@...
	@mkdir -p $$(LIB_DIR)
	$$(GCC_AR) $$(ARFLAGS) $$@ $(1).OBJ
endef

define define_standard_lib
$(1).SRC := $$(sort $$(wildcard $$(MODULE_SRC_DIR)/*.cc))
$(1).OBJ := $$($(1).SRC:$$(MODULE_SRC_DIR)/%.cc=$$(MODULE_BUILD_DIR)/%.o)
$$(eval $$(call define_lib,$(1)))
endef
