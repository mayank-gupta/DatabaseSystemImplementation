/*
 * SelectPipe.h
 *
 *      Author: Mayank Gupta
 */

#ifndef SELECTPIPE_H_
#define SELECTPIPE_H_

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "RelOp.h"

class SelectPipe : public RelationalOp {

  public:
    pthread_t workerThread;
    Pipe* inPipe;
    Pipe* outPipe;
    Record* literal;
    CNF* cnf;
    int runlen;
    void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};

#endif /* SELECTPIPE_H_ */
