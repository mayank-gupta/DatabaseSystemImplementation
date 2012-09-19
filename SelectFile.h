/*
 * SelectFile.h
 *
 *      Author: Mayank Gupta
 */

#ifndef SELECTFILE_H_
#define SELECTFILE_H_

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "RelOp.h"

class SelectFile : public RelationalOp {

  public:
    pthread_t workerThread;
    int runlen;
    DBFile* dbFile;
    Pipe* outPipe;
    CNF* cnf;
    Record* literal;
    void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone ();
    void Use_n_Pages (int n);

};

#endif /* SELECTFILE_H_ */
