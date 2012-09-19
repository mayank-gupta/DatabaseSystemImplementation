/*
 * Project.h
 *
 *      Author: Mayank Gupta
 */

#ifndef PROJECT_H_
#define PROJECT_H_

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "RelOp.h"


class Project : public RelationalOp {
  public:
    pthread_t workerThread;
    Pipe* inPipe;
    Pipe* outPipe;
    int* keepme;
    int numAttsInput;
    int numAttsOutput;
    int runlen;
    void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};

#endif /* PROJECT_H_ */
