/*
 * SelectPipe.cc
 *
 *      Author: Mayank Gupta
 */

#include "SelectPipe.h"

void* selectPipeThreadFunction(void* obj) {
  printf("Entered Select Pipe worker function\n");
  SelectPipe* sp = (SelectPipe*) obj;
  ComparisonEngine ce;
  Record* record = new Record();

  while(sp->inPipe->Remove(record)){
    if (ce.Compare(record, sp->literal, sp->cnf) == 1) {
      printf("Matched");
      //record->Print(&mySchema);
      sp->outPipe->Insert(record);
      record = new Record();
    }
  }

  printf("select pipe worker finished its job\n");
  sp->outPipe->ShutDown();
  delete record;

}
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
  printf("Run SelectPipe called\n");
  this->inPipe = &inPipe;
  this->outPipe = &outPipe;
  this->literal = &literal;
  this->cnf = &selOp;

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_create(&workerThread, &attr, selectPipeThreadFunction, (void*) this);

}

void SelectPipe::WaitUntilDone () {
  pthread_join(workerThread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {

  this->runlen = runlen;
}
