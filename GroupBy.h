

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <malloc.h>
#include <fstream>
#include <stdio.h>
#include <iostream>
#include <math.h>
#include <string.h>
#include <sstream>


class GroupBy: public RelationalOp {
  private:
    Pipe *gb_inputPipe;
    Pipe *gb_outputPipe;
    OrderMaker *gb_orderMaker;
    Function *gb_computeMe;
    pthread_t gb_workerThread;
    int gb_runlen;
    Schema *objsch;
  public:
    GroupBy() {
    }
    ;
    ~GroupBy() {
    }
    ;
    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
	Function &computeMe, Schema *objsch);
    void WaitUntilDone();
    void GroupByOperation();
    void Use_n_Pages(int n);
};

