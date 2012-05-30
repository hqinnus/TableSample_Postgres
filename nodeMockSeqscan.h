/*-------------------------------------------------------------------------
 *
 * nodeMockSeqscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeMockSeqscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMOCKSEQSCAN_H
#define NODEMOCKSEQSCAN_H

#include "nodes/execnodes.h"

extern MockSeqScanState *ExecInitMockSeqScan(MockSeqScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecMockSeqScan(MockSeqScanState *node);
extern void ExecEndMockSeqScan(MockSeqScanState *node);
extern void ExecMockSeqMarkPos(MockSeqScanState *node);
extern void ExecMockSeqRestrPos(MockSeqScanState *node);
extern void ExecReScanMockSeqScan(MockSeqScanState *node);

#endif   /* NODEMOCKSEQSCAN_H */
