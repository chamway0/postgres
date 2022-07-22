/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr.h"
#include "storage/buf_internals.h"


/* entry for buffer lookup hashtable */
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	int			id;				/* Associated buffer ID */
} BufferLookupEnt;

static HTAB *SharedBufHash;

/*
使用数组实现散列表
*/
#define USE_LOOKUP_2  0
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	int			id;				/* Associated buffer ID */
	int			next;           /* 用于解决键冲突 */		
} BufferLookupEnt2;


typedef struct
{
    uint32	size;
	uint32	mask;
	uint32  conflict_index;
	uint32  buf_id;
	BufferLookupEnt2	*hash;
	uint32	*free_buf_id;	
	LWLock		lock;	
} BufferLookupTable;


static BufferLookupTable *LookupTable;

static void insert_to_head(uint32 id)
{
	//取到对应节点
	uint32 *node = &LookupTable->free_buf_id[id];

	//将该节点指向第一个节点
	*node = LookupTable->buf_id;

	//链表头指向该节点，使其成为第一个节点
	LookupTable->buf_id = id;
}
static uint32 get_from_head()
{
	//返回第一个节点并从链表删除
	uint32 id = LookupTable->buf_id;

	//取到第二个节点
	uint32 next = LookupTable->free_buf_id[id];

	//链表头指向该节点
	LookupTable->buf_id = next;

	return id;
}
inline static bool ftag_equal(BufferTag *tag1,BufferTag *tag2)
{
	return (( tag1->rnode.spcNode == tag2->rnode.spcNode) && 
			( tag1->rnode.dbNode  == tag2->rnode.dbNode) &&
			( tag1->rnode.relNode == tag2->rnode.relNode) &&
			( tag1->forkNum  == tag2->forkNum) &&
			( tag1->blockNum == tag2->blockNum) );
}

/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 */
Size
BufTableShmemSize(int size)
{
	if( share_buffer_type == 1)
		return hash_estimate_size(size, sizeof(BufferLookupEnt));
	else
	{
#if USE_LOOKUP_2
		return (sizeof(BufferLookupTable) + 2*size*sizeof(BufferLookupEnt2) + size*sizeof(uint32));
#else
		return hash_estimate_size(size, sizeof(BufferLookupEnt));
#endif
	}
		
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 */
void
InitBufTable(int size)
{
	if( share_buffer_type == 1)
	{
		HASHCTL		info;

		/* assume no locking is needed yet */

		/* BufferTag maps to Buffer */
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(BufferLookupEnt);
		info.num_partitions = NUM_BUFFER_PARTITIONS;

		SharedBufHash = ShmemInitHash("Shared Buffer Lookup Table",
									size, size,
									&info,
									HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
	}
	else
	{
#if USE_LOOKUP_2
		bool	found;

		/* look it up in the shmem index */
		LookupTable = ShmemInitStruct("Shared Buffer Lookup Table",
								BufTableShmemSize(size),
								&found);	

		if(!found)
		{
			LookupTable->size = size;
			LookupTable->mask = ((1 << my_log2(size)) << 1) - 1;
			LookupTable->conflict_index = size;			
			LookupTable->hash = (BufferLookupEnt2*)((char*)LookupTable + sizeof(BufferLookupTable));
			LookupTable->free_buf_id = (uint32*)((char*)LookupTable->hash + 2*size*sizeof(BufferLookupEnt2));
			LookupTable->buf_id = 0;

			//初始化hash表
			for(int i=0; i<size*2; i++)
			{
				LookupTable->hash[i].id = -1;
				LookupTable->hash[i].next = -1;
			}

			//初始化空闲链表				
			for(int i=0; i<size; i++)
			{
				LookupTable->free_buf_id[i] = i+1;
			}	
			LookupTable->free_buf_id[size-1] = -1;
		}	
#else
		HASHCTL		info;

		/* assume no locking is needed yet */

		/* BufferTag maps to Buffer */
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(BufferLookupEnt);
		info.num_partitions = NUM_BUFFER_PARTITIONS;

		SharedBufHash = ShmemInitHash("Shared Buffer Lookup Table",
									size, size,
									&info,
									HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
#endif		
	}
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	if(share_buffer_type == 1)
		return get_hash_value(SharedBufHash, (void *) tagPtr);
	else
	{
#if USE_LOOKUP_2
		return tag_hash(tagPtr,sizeof(BufferTag));
#else
		return get_hash_value(SharedBufHash, (void *) tagPtr);
#endif		
	}
		
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	if( share_buffer_type == 1)
	{
		BufferLookupEnt *result;

		result = (BufferLookupEnt *)
			hash_search_with_hash_value(SharedBufHash,
										(void *) tagPtr,
										hashcode,
										HASH_FIND,
										NULL);

		if (!result)
			return -1;

		return result->id;
	}
	else
	{
#if USE_LOOKUP_2		
		uint32 bucket = hashcode & LookupTable->mask;
		int result = -1;

		for(;;)
		{
			BufferLookupEnt2 *e = &LookupTable->hash[bucket];
			if( e->id >= 0)
			{
				if( ftag_equal(&e->key,tagPtr) )
				{
					result = e->id;
					break;
				}
			}

			if( e->next < 0 )
				break;
			
			bucket = e->next;
		}

		return 	result;
#else
		BufferLookupEnt *result;

		result = (BufferLookupEnt *)
			hash_search_with_hash_value(SharedBufHash,
										(void *) tagPtr,
										hashcode,
										HASH_FIND,
										NULL);

		if (!result)
			return -1;

		return result->id;
#endif
	}

	return -1;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int *buf_id)
{
	if( share_buffer_type == 1)
	{
		BufferLookupEnt *result;
		bool		found;

		Assert(*buf_id >= 0);		/* -1 is reserved for not-in-table */
		Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

		result = (BufferLookupEnt *)
			hash_search_with_hash_value(SharedBufHash,
										(void *) tagPtr,
										hashcode,
										HASH_ENTER,
										&found);

		if (found)					/* found something already in the table */
			return result->id;

		result->id = *buf_id;

		return -1;
	}
	else
	{
#if USE_LOOKUP_2
		uint32 bucket;
		uint32 free_buf_id;
		int result;


		result = BufTableLookup(tagPtr,hashcode);
		if( result >= 0 )
			return result;

		bucket = hashcode & LookupTable->mask;
		LWLockAcquire(&LookupTable->lock, LW_EXCLUSIVE);
		for(;;)
		{
			BufferLookupEnt2* e = &LookupTable->hash[bucket];
			if( e->id >= 0)
			{
				if( ftag_equal(&e->key,tagPtr))
				{
					//其他进程已经插入了
					result = e->id;
					break;
				}

				if( e->next >= 0 )
				{
					bucket = e->next;
					continue;
				}

				e->next = LookupTable->conflict_index++;
				bucket = e->next;

				e = &LookupTable->hash[bucket];
			}

			free_buf_id = get_from_head();
			if( free_buf_id < 0 )
			{
				elog(ERROR,"Lookup Table out of memory! no free buf_id");
				break;
			}		

			*buf_id = free_buf_id;
			e->id = free_buf_id;
			e->key = *tagPtr;
			break;
		}		

		LWLockRelease(&LookupTable->lock);
		return result;	
#else
		BufferLookupEnt *result;
		bool		found;

		Assert(*buf_id >= 0);		/* -1 is reserved for not-in-table */
		Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

		result = (BufferLookupEnt *)
			hash_search_with_hash_value(SharedBufHash,
										(void *) tagPtr,
										hashcode,
										HASH_ENTER,
										&found);

		if (found)					/* found something already in the table */
			return result->id;

		result->id = *buf_id;

		return -1;
#endif
	}

	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	#if !(USE_LOOKUP_2)
	{
		BufferLookupEnt *result;

		result = (BufferLookupEnt *)
			hash_search_with_hash_value(SharedBufHash,
										(void *) tagPtr,
										hashcode,
										HASH_REMOVE,
										NULL);

		if (!result)				/* shouldn't happen */
			elog(ERROR, "shared buffer hash table corrupted");
	}
	#endif
	// else
	// {
	// 	uint32 bucket;
	// 	uint32 free_buf_id;


	// 	bucket = hashcode & LookupTable->mask;
	// 	LWLockAcquire(&LookupTable->lock, LW_EXCLUSIVE);
	// 	for(;;)
	// 	{
	// 		BufferLookupEnt2 *e = &LookupTable->data[bucket];
	// 		if( e->id >= 0)
	// 		{
	// 			if( ftag_equal(&e->key,tagPtr) )
	// 			{
	// 				free_buf_id = e->id;
	// 				e->id = -1;

	// 				insert_to_head(free_buf_id);
	// 				break;
	// 			}
	// 		}

	// 		if( e->next < 0 )
	// 			break;

	// 		bucket = e->next;
	// 	}		

	// 	LWLockRelease(&LookupTable->lock);			
	// }
}
