/*-------------------------------------------------------------------------
 *
 * nodesamplescan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodesamplescan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESAMPLESCAN_H
#define NODESAMPLESCAN_H

#include "nodes/execnodes.h"

extern SampleScanState *ExecInitSampleScan(SampleScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecSampleScan(SampleScanState *node);
extern void ExecEndSampleScan(SampleScanState *node);
extern void ExecSampleMarkPos(SampleScanState *node);
extern void ExecSampleRestrPos(SampleScanState *node);
extern void ExecReScanSampleScan(SampleScanState *node);

#endif   /* NODESAMPLESCAN_H */
