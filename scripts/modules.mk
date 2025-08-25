# Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.

# Canned recipe to set up the environment variables for including a single
# module and executing its module.mk file.  This then recurses through all the
# newly-defined SUBMODULES and does the same thing for each of them.
#
# The modules.mk file has access to the following environment variables:
# 	$(MODULE_SRC_DIR)    - the path to the module.mk's source directory.
# 	$(PARENT_SRC_DIR)    - the path to the module.mk's parent source
# 	                       directory.
# 	$(MODULE_BUILD_DIR)  - the path to the module.mk's build directory.
# 	$(PARENT_BUILD_DIR)  - the path to the module.mk's parent build
# 	                       directory.
# 	$(MODULE_TBUILD_DIR) - the path to the module.mk's unittest build
# 	                       directory.
# 	$(PARENT_TBUILD_DIR) - the path to the module.mk's unittest parent
# 	                       build directory.
#
# The module.mk file may define the following variables to control further
# execution:
# 	SUBMODULES := a list of subdirectories with module.mk files.
# 	TESTS      := a list of test binaries to build.
# 	ITESTS     := a list of integration test binaries to build.
# 	HEADERS    += a list of directories to add to the root headers -
# 	              typically this is simply $(MODULE_SRC_DIR) when used.
#
# Parameters to include_module:
# 	$(1) - relative path to the module directory from $(SRC_DIR)
# 	$(2) - relative path to the module's parent directory from $(SRC_DIR)
define include_module
	$(eval SUBMODULES := )
	$(eval TESTS := )
	$(eval ITESTS := )
	$(eval MODULE := $(1))
	$(eval PARENT := $(2))
	$(eval MODULE_SRC_DIR := $(SRC_DIR)/$(MODULE))
	$(eval PARENT_SRC_DIR := $(SRC_DIR)/$(PARENT))
	$(eval MODULE_BUILD_DIR := $(BUILD_O_DIR)/$(MODULE))
	$(eval PARENT_BUILD_DIR := $(BUILD_O_DIR)/$(PARENT))
	$(eval MODULE_TBUILD_DIR := $(BUILD_TO_DIR)/$(MODULE))
	$(eval PARENT_TBUILD_DIR := $(BUILD_TO_DIR)/$(PARENT))
	$(eval MODULE_ITBUILD_DIR := $(BUILD_ITO_DIR)/$(MODULE))
	$(eval PARENT_ITBUILD_DIR := $(BUILD_ITO_DIR)/$(PARENT))
	$(eval MODULE_MK := $(MODULE_SRC_DIR)/module.mk)
	$(eval include $(MODULE_MK))
	$(eval $(foreach T,$(TESTS),$(eval $(T).MK := $(MODULE_MK))))
	$(eval ALL_TESTS += $(TESTS))
	$(eval $(foreach T,$(ITESTS),$(eval $(T).MK := $(MODULE_MK))))
	$(eval ALL_ITESTS += $(ITESTS))
	ifneq ($(strip $(SUBMODULES)),)
		$(eval $(call include_modules,$(patsubst %,$(1)/%,$(SUBMODULES)),$(1)))
	endif
endef

# Canned recipe to include all modules from a list of modules.
# Parameters:
# 	$(1) - the list of module subdirectories.
# 	$(2) - the common prefix, relative to $(SRC_DIR) for the $(1) list.
define include_modules
	$(eval $(foreach M,$(1),$(call include_module,$(M),$(2))))
endef
