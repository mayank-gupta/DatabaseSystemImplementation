#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "BigQ.h"
#include <string>
#include <fstream>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <vector>



class RelationalOp {
  public:
    // blocks the caller until the particular relational operator 
    // has run to completion
    virtual void WaitUntilDone () = 0;

    // tell us how much internal memory the operation can use
    virtual void Use_n_Pages (int n) = 0;
};

class GroupBy : public RelationalOp {
  private:
    int n;
    pthread_t groupby_thread;
  public:
    OrderMaker groupAtts;
    Function computeMe;
    void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};

#endif
