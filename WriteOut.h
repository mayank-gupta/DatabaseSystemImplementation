/*
 * WriteOut.h
 *
 *      Author: Mayank Gupta
 */

#ifndef WRITEOUT_H_
#define WRITEOUT_H_

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "RelOp.h"

class WriteOut : public RelationalOp {
  public:
    pthread_t workerThread;
    Pipe* inPipe;
    FILE* outFile;
    Schema* schema;
    int runlen;
    void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};
#endif /* WRITEOUT_H_ */
