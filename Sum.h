/*
 * Sum.h
 *
 *      Author: Mayank Gupta
 */

#ifndef SUM_H_
#define SUM_H_


#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "RelOp.h"
#include <string.h>

void *SumWorker(void *);
class Sum : public RelationalOp {
  public:
    pthread_t thread;
    //sRecord *buffer;
    Pipe *myInputPipe;
    Pipe *myOutputPipe;
    Function *toCompute;
    void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};
#endif /* SUM_H_ */
