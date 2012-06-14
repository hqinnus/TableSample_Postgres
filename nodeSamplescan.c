/*-------------------------------------------------------------------------
 *
 * nodeSamplescan.c
 *	  Support routines for sample scans of relations.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSamplescan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSampleScan				sequentially scans a relation.
 *		ExecSampleNext				retrieve next tuple in sequential order.
 *		ExecInitSampleScan			creates and initializes a seqscan node.
 *		ExecEndSampleScan			releases any storage allocated.
 *		ExecReScanSampleScan		rescans the relation
 *		ExecSampleMarkPos			marks scan position
 *		ExecSampleRestrPos			restores scan position
 */
#include "postgres.h"

#include <time.h>

#include "access/relscan.h"
#include "executor/execdebug.h"
#include "executor/nodeSamplescan.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "executor/executor.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"

static void InitScanRelation(SampleScanState *node, EState *estate);
static TupleTableSlot *SampleNext(SampleScanState *node);
static void LoadNextSampleBuffer(SampleScanState *node);
static int get_rand_in_range(int a, int b);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SampleNext
 *
 *		This is a workhorse for ExecSampleScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SampleNext(SampleScanState *node)
{
	//HeapTuple	tuple;
	//HeapScanDesc scandesc;
	EState	   *estate;
	//ScanDirection direction;
	TupleTableSlot *slot;
	Relation	   rel;
	Index		   scanrelid;

	/*
	 * get information from the estate and scan state
	 */
	//scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	//direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;
	rel  = node->ss.ss_currentRelation;
	scanrelid = ((SampleScan *) node->ss.ps.plan)->scan.scanrelid;

	while(true)
	{
		OffsetNumber	max_offset;
		Page			page;

		/*
		 * If we don't have a valid buffer, choose the next block to
		 * sample and load it into memory.
		 */
		if (node->need_new_buf)
		{
			LoadNextSampleBuffer(node);
			node->need_new_buf = false;

			/* We're out of blocks in the rel, so we're done */
			if (!BufferIsValid(node->cur_buf))
				break;
		}

		/*
		 * Iterate through the current block, checking for heap tuples
		 * that are visible to our transaction. Return each such 
		 * candidate match:ExecScan() takes care of checking whether
		 * the tuple satisfies the scan's quals.
		 */
		LockBuffer(node->cur_buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(node->cur_buf);
		max_offset = PageGetMaxOffsetNumber(page);
		while (node->cur_offset <= max_offset)
		{
			/*
			 * Postgres uses a somewhat unusual API for specifying the
			 * location of the tuple we want to fetch. We've already
			 * allocated space for a HeapTupleData; to indicate the TID
			 * we want to fetch into the HeapTuple, we fillin its "t_self"
			 * field, and then ask the heap access manager to fetch the
			 * tuple's data for us. 
			 */
			ItemPointerSet(&node->cur_tup.t_self,
						   node->cur_blkno, node->cur_offset);

			node->cur_offset++;

			if (heap_fetch(rel, estate->es_snapshot,
								   &node->cur_tup, &node->cur_buf,
								   true, NULL))
			{
				LockBuffer(node->cur_buf, BUFFER_LOCK_UNLOCK);

				/*
				 * save the tuple and the buffer returned to us by the access methods in
				 * our scan tuple slot and return the slot.  Note: we pass 'false' because
				 * tuples returned by heap_getnext() are pointers onto disk pages and were
				 * not created with palloc() and so should not be pfree()'d.  Note also
				 * that ExecStoreTuple will increment the refcount of the buffer; the
				 * refcount will not be dropped until the tuple table slot is cleared.
				 */
				ExecStoreTuple(&node->cur_tup,
							   slot,
							   node->cur_buf,
							   false);

				return slot;
			}
		}
		
		/*
		 * Out of tuples on this page, so we're done; clear result slot
		 */
		LockBuffer(node->cur_buf, BUFFER_LOCK_UNLOCK);
		node->need_new_buf = true;
	}

	/* No more blocks to scan, so we're done; clear result slot. */
	ExecClearTuple(slot);
	return NULL;
}

/*
 * Choose the next block from the relation to sample. This is called when
 * (a) we haven't sampled any blocks from the relation yet(SampleScanState.cur_buf ==
 * InvalidBuffer) (b)we've examined every tuple in the block we're currently sampling.
 *
 * If we've run out of blocks in the relation, we leave 'cur_buf' as InvalidBuffer.
 */
static void
LoadNextSampleBuffer(SampleScanState *node)
{
	SampleScan *plan_node = (SampleScan *)node->ss.ps.plan;

	while(true)
	{
		int rand_percent;

		/*
		 * If this is the first time through, start at the beginning of the heap.
		 */
		if (BlockNumberIsValid(node->cur_blkno))
			node->cur_blkno++;
		else
			node->cur_blkno = 0;

		rand_percent = get_rand_in_range(0, 100);

		if(rand_percent >= plan_node->sample_info->sample_percent)
			continue;

		/*
		 * If we've reached the end of the heap, we're done. Make sure to unpin
		 * the current buffer, if any.
		 */
		 if(node->cur_blkno >= node->nblocks)
		{
			if(BufferIsValid(node->cur_buf))
			{
				ReleaseBuffer(node->cur_buf);
				node->cur_buf = InvalidBuffer;
			}

			break;
		}

		/*
		 * Okay, we've chosen another block to read: ask buffer manager to load
		 * it into the buffer pool for us, pin it, and release the pin we hold
		 * on the previous "cur_buf". For the case that "cur_buff" == InvalidBuffer,
		 * ReleaseAndReadBuffer() is equivalent to ReadBuffer().
		 */
		node->cur_buf = ReleaseAndReadBuffer(node->cur_buf,
											 node->ss.ss_currentRelation,
											 node->cur_blkno);
		node->cur_offset = FirstOffsetNumber;
		break;
	}
}

/*
 * Returns a randomly-generated trigger x, such that a <= x < b
 */
static int
get_rand_in_range(int a, int b)
{
	/*
	 * XXX: Using modulus takes the low-order bits of the random
	 * number; since the high-order bits may contain more entropy
	 * with more PRNGs, we should probably use those instead.
	 */
	return (random() % b) + a;
}

/*
 * SampleRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SampleRecheck(SampleScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SampleScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSampleScan(node)
 *
 *		Return the next tuple in the sample scan's result set
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecSampleScan(SampleScanState *node)
{
	char *prev_state;
	
	PG_TRY();
	{
		/* Install our PRNG state */
		prev_state = setstate(node->rand_state);

		return ExecScan((ScanState *) node,
					(ExecScanAccessMtd) SampleNext,
					(ExecScanRecheckMtd) SampleRecheck);
	}
	PG_CATCH();
	{
		setstate(prev_state);
		PG_RE_THROW();
	}
	PG_END_TRY();

	setstate(prev_state);
}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		This does the initialization for scan relations and
 *		subplans of scans.
 * ----------------------------------------------------------------
 */
static void
InitScanRelation(SampleScanState *node, EState *estate)
{
	Relation	currentRelation;
	HeapScanDesc currentScanDesc;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
									 ((SampleScan *) node->ss.ps.plan)->scan.scanrelid);

	currentScanDesc = heap_beginscan(currentRelation,
									 estate->es_snapshot,
									 0,
									 NULL);

	/*                 
	 * Determine the number of blocks in the relation. We need only do
	 * this once for a given scan: if any new blocks are added to the
	 * relation, they won't be visible to this transaction anyway.
	 */                
	node->nblocks = RelationGetNumberOfBlocks(currentRelation);

	node->ss.ss_currentRelation = currentRelation;
	node->ss.ss_currentScanDesc = currentScanDesc;

	ExecAssignScanType(&node->ss, RelationGetDescr(currentRelation));
}

/* ----------------------------------------------------------------
 *		ExecInitSampleScan
 * ----------------------------------------------------------------
 */
SampleScanState *
ExecInitSampleScan(SampleScan *node, EState *estate, int eflags)
{
	SampleScanState *scanstate;

	/*
	 * We don't expect to have any child plan node
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SampleScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->cur_buf		 = InvalidBuffer;
	scanstate->cur_offset	 = FirstOffsetNumber;
	scanstate->cur_blkno	 = InvalidBlockNumber;
	scanstate->need_new_buf  = true;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) scanstate);

	/*
	 * This part seems no use anymore, and the ExecCountSlotsSampleScan is not in execProcNode.c
	 * any more.
     #define SAMPLESCAN_NSLOTS 2
	 */

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &scanstate->ss);

	/*
	 * initialize scan relation
	 */
	InitScanRelation(scanstate, estate);

	scanstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * There is repeatable support code section from Neil's code
	 * here. Will add them later.
	 */

	return scanstate;
}

/* ----------------------------------------------------------------
 *						Join Support
 * To understand this part more, read at 
 * 1. execAmi.c, ExecSupportsMarkRestore
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSampleScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSampleScan(SampleScanState *node)
{
	HeapScanDesc scan;

	scan = node->ss.ss_currentScanDesc;

	heap_rescan(scan,			/* scan desc */
				NULL);			/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *		ExecSampleMarkPos(node)
 *
 *		Marks scan position.
 * ----------------------------------------------------------------
 */
void
ExecSampleMarkPos(SampleScanState *node)
{
	HeapScanDesc scan = node->ss.ss_currentScanDesc;

	heap_markpos(scan);
}

/* ----------------------------------------------------------------
 *		ExecSampleRestrPos
 *
 *		Restores scan position.
 * ----------------------------------------------------------------
 */
void
ExecSampleRestrPos(SampleScanState *node)
{
	HeapScanDesc scan = node->ss.ss_currentScanDesc;

	/*
	 * Clear any reference to the previously returned tuple.  This is needed
	 * because the slot is simply pointing at scan->rs_cbuf, which
	 * heap_restrpos will change; we'd have an internally inconsistent slot if
	 * we didn't do this.
	 */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	heap_restrpos(scan);
}

/*
 * Shutdown this scan. This function should generally be symmetric with
 * ExecInitSampleScan(): we ought to clean up after ourselves.
 */
void
ExecEndSampleScan(SampleScanState *node)
{
	setstate(node->prev_rand_state);

	ExecFreeExprContext(&node->ss.ps);

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	if (BufferIsValid(node->cur_buf))
	{
		ReleaseBuffer(node->cur_buf);
		node->cur_buf = InvalidBuffer;
	}

	/*
	 * Note that ExecCloseScanRelation() does NOT release the lock we
	 * acquired on the scan relation: it is held until the end of the
	 * transaction.
	 */
	ExecCloseScanRelation(node->ss.ss_currentRelation);
}
