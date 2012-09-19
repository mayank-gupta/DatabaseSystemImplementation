/*
 * Join.h
 *
 *      Author: Mayank Gupta
 */

#ifndef JOIN_H_
#define JOIN_H_

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "RelOp.h"

class Join : public RelationalOp {
  public:
    pthread_t workerThread;
    Pipe* inPipeLeft;
    Pipe* inPipeRight;
    Pipe* outPipe;
    CNF* cnf;
    Record* literal;
    OrderMaker* orderMakerLeft;
    OrderMaker* orderMakerRight;
    Pipe* bigqOutPipeLeft;
    Pipe* bigqOutPipeRight;
    int runlen;
    Join(){
      runlen = 50;
    }
    void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};


#endif /* JOIN_H_ */
