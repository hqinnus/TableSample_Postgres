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
	HeapTuple	tuple;
	HeapScanDesc scandesc;
	TupleTableSlot *slot;
	SampleScan *plan_node = (SampleScan *) node->ss.ps.plan;
	TableSampleMethod sample_method = plan_node->sample_info->sample_method;
	int				  sample_percent = plan_node->sample_info->sample_percent;
	BernoulliSampler	  bs = node->bsampler;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	slot = node->ss.ss_ScanTupleSlot;

	tuple = heap_getnext_samplescan(scandesc, sample_percent,
								sample_method, bs);

	/*              
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note: we pass 'false' because
	 * tuples returned by heap_getnext() are pointers onto disk pages and were
	 * not created with palloc() and so should not be pfree()'d.  Note also
	 * that ExecStoreTuple will increment the refcount of the buffer; the
	 * refcount will not be dropped until the tuple table slot is cleared.
	 */
	if (tuple)
		ExecStoreTuple(tuple,	/* tuple to store */
					   slot,	/* slot to store in */
					   scandesc->rs_cbuf,		/* buffer associated with this
												 * tuple */
					   false);	/* don't pfree this pointer */
	else
		ExecClearTuple(slot);

	return slot;
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
	TableSampleMethod sample_method = node->sample_info->sample_method;
	int				  sample_percent = node->sample_info->sample_percent;
	BernoulliSamplerData bs;
	int				  seed;

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
	scanstate->bsampler = &bs;

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
	 * Initialize fields of SampleScanState for BERNOULLI
	 */
	if(sample_method == SAMPLE_BERNOULLI)
	{
		HeapScanDesc scan = scanstate->ss.ss_currentScanDesc;
		double numtuples = scan->rs_rd->rd_rel->reltuples;
		double row_test = numtuples*sample_percent/100;
		int targrows = (int)floor(row_test + 0.5);

		scan->targrows = targrows;
		scan->rs_samplerows = (HeapTuple *)palloc(targrows * sizeof(HeapTuple));
		scan->rs_curindex = 0;
		scan->rs_samplesize = 0;
		scan->rs_sampleinited = false;
	}

	/*
	 * There is repeatable support code section from Neil's code
	 * here. Will add them later.
	 */
	seed = (int) time(NULL);

#define RAND_STATE_SIZE 128

	scanstate->rand_state = (char *) palloc(sizeof(char) * RAND_STATE_SIZE);
	initstate(seed, scanstate->rand_state, RAND_STATE_SIZE);

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

	/*
	 * Close heap scan
	 */
	heap_endscan(node->ss.ss_currentScanDesc);

	/*
	 * Note that ExecCloseScanRelation() does NOT release the lock we
	 * acquired on the scan relation: it is held until the end of the
	 * transaction.
	 */
	ExecCloseScanRelation(node->ss.ss_currentRelation);
}
