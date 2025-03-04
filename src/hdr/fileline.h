// Copyright (c) 2018-2020 by Terry Greeniaus.  All rights reserved.
#ifndef __ARM_DEV_ENVIRONMENT_FILELINE_H
#define __ARM_DEV_ENVIRONMENT_FILELINE_H

#define _LINESTR(l) #l
#define LINESTR(l) _LINESTR(l)
#define FILELINESTR __FILE__ ":" LINESTR(__LINE__)

#endif /* __ARM_DEV_ENVIRONMENT_FILELINE_H */
