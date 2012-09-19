/*
 * DuplicateRemoval.h
 *
 *      Author: Mayank Gupta
 */

#ifndef DUPLICATEREMOVAL_H_
#define DUPLICATEREMOVAL_H_

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "RelOp.h"

class DuplicateRemoval : public RelationalOp {
  public:
    pthread_t workerThread;
    Pipe* inPipe;
    Pipe* outPipe;
    Schema* schema;
    OrderMaker* ordermaker;
    Pipe* biqOutPipe;
    int runlen;
    DuplicateRemoval(){
      runlen=5;
    }
    void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};

#endif /* DUPLICATEREMOVAL_H_ */
