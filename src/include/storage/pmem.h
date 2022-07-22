/*-------------------------------------------------------------------------
+ *
+ * pmem.h
+ *		Virtual file descriptor definitions for persistent memory.
+ *
+ *
+ * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
+ * Portions Copyright (c) 1994, Regents of the University of California
+ *
+ * src/include/storage/pmem.h
+ *
+ *-------------------------------------------------------------------------
+ */

#ifndef PMEM_H
#define PMEM_H

#include "postgres.h"
#include "miscadmin.h"

#define NO_FD_FOR_MAPPED_FILE -2
#define USE_LIBPMEM 1
#define PG_FILE_MODE_DEFAULT	(S_IRUSR | S_IWUSR)

extern bool CheckPmem(const char *path);
extern int PmemFileOpen(const char *pathname, int flags, size_t fsize,
			 void **addr,size_t *mapped_len);
extern int PmemFileOpenPerm(const char *pathname, int flags, int mode,
				 size_t fsize, void **addr,size_t *mapped_len);
extern void PmemFileWrite(void *dest, void *src, size_t len);
extern void PmemFileRead(void *map_addr, void *buf, size_t len);
extern void PmemFileFlush(void *map_addr, size_t len);
extern void PmemFileSync(void);
extern int	PmemFileClose(void *addr, size_t fsize);
extern void PmemFileMemset(void *addr, int c, size_t len);

#endif							/* PMEM_H */