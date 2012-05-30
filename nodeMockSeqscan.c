/*-------------------------------------------------------------------------
 *
 * nodeMockSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMockSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecMockSeqScan				sequentially scans a relation.
 *		ExecMockSeqNext				retrieve next tuple in sequential order.
 *		ExecInitMockSeqScan			creates and initializes a seqscan node.
 *		ExecEndMockSeqScan			releases any storage allocated.
 *		ExecReScanMockSeqScan		rescans the relation
 *		ExecMockSeqMarkPos			marks scan position
 *		ExecMockSeqRestrPos			restores scan position
 */
#include "postgres.h"

#include "access/relscan.h"
#include "executor/execdebug.h"
#include "executor/nodeMockSeqscan.h"
#include "utils/rel.h"

static void InitScanRelation(MockSeqScanState *node, EState *estate);
static TupleTableSlot *MockSeqNext(MockSeqScanState *node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		MockSeqNext
 *
 *		This is a workhorse for ExecMockSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
MockSeqNext(MockSeqScanState *node)
{
	HeapTuple	tuple;
	HeapScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss_currentScanDesc;
	estate = node->ps.state;
	direction = estate->es_direction;
	slot = node->ss_ScanTupleSlot;

	/*
	 * get the next tuple from the table
	 */
	tuple = heap_getnext(scandesc, direction);

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
 * MockSeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
MockSeqRecheck(MockSeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, MockSeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecMockSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecMockSeqScan(MockSeqScanState *node)
{
	return ExecScan((ScanState *) node,
					(ExecScanAccessMtd) MockSeqNext,
					(ExecScanRecheckMtd) MockSeqRecheck);
}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		This does the initialization for scan relations and
 *		subplans of scans.
 * ----------------------------------------------------------------
 */
static void
InitScanRelation(MockSeqScanState *node, EState *estate)
{
	Relation	currentRelation;
	HeapScanDesc currentScanDesc;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
									 ((MockSeqScan *) node->ps.plan)->scanrelid);

	currentScanDesc = heap_beginscan(currentRelation,
									 estate->es_snapshot,
									 0,
									 NULL);

	node->ss_currentRelation = currentRelation;
	node->ss_currentScanDesc = currentScanDesc;

	ExecAssignScanType(node, RelationGetDescr(currentRelation));
}


/* ----------------------------------------------------------------
 *		ExecInitMockSeqScan
 * ----------------------------------------------------------------
 */
MockSeqScanState *
ExecInitMockSeqScan(MockSeqScan *node, EState *estate, int eflags)
{
	MockSeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a MockSeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(MockSeqScanState);
	scanstate->ps.plan = (Plan *) node;
	scanstate->ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ps);

	/*
	 * initialize child expressions
	 */
	scanstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) scanstate);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ps);
	ExecInitScanTupleSlot(estate, scanstate);

	/*
	 * initialize scan relation
	 */
	InitScanRelation(scanstate, estate);

	scanstate->ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ps);
	ExecAssignScanProjectionInfo(scanstate);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndMockSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndMockSeqScan(MockSeqScanState *node)
{
	Relation	relation;
	HeapScanDesc scanDesc;

	/*
	 * get information from node
	 */
	relation = node->ss_currentRelation;
	scanDesc = node->ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	heap_endscan(scanDesc);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanMockSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanMockSeqScan(MockSeqScanState *node)
{
	HeapScanDesc scan;

	scan = node->ss_currentScanDesc;

	heap_rescan(scan,			/* scan desc */
				NULL);			/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *		ExecMockSeqMarkPos(node)
 *
 *		Marks scan position.
 * ----------------------------------------------------------------
 */
void
ExecMockSeqMarkPos(MockSeqScanState *node)
{
	HeapScanDesc scan = node->ss_currentScanDesc;

	heap_markpos(scan);
}

/* ----------------------------------------------------------------
 *		ExecMockSeqRestrPos
 *
 *		Restores scan position.
 * ----------------------------------------------------------------
 */
void
ExecMockSeqRestrPos(MockSeqScanState *node)
{
	HeapScanDesc scan = node->ss_currentScanDesc;

	/*
	 * Clear any reference to the previously returned tuple.  This is needed
	 * because the slot is simply pointing at scan->rs_cbuf, which
	 * heap_restrpos will change; we'd have an internally inconsistent slot if
	 * we didn't do this.
	 */
	ExecClearTuple(node->ss_ScanTupleSlot);

	heap_restrpos(scan);
}
