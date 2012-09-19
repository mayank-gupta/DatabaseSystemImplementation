/*
 * DuplicateRemoval.cc
 *
 *      Author: Mayank Gupta
 */


#include "DuplicateRemoval.h"

void* duplicateRemovalThreadFunction(void* obj){
  printf("Entered duplicate removal thread function\n");
  DuplicateRemoval* dr = (DuplicateRemoval*) obj;

  BigQ bigq(*(dr->inPipe),*(dr->biqOutPipe),*(dr->ordermaker),(dr->runlen));
  ComparisonEngine ce;
  Record *prev = new Record();
  Record *current = new Record();
  dr->biqOutPipe->Remove(prev);
  while(dr->biqOutPipe->Remove(current)){


    fflush(stdout);
    if(ce.Compare(prev,current,dr->ordermaker)!=0){
      dr->outPipe->Insert(prev);
      prev = new Record();
      prev->Consume(current);
      current = new Record();
    }
  }
  dr->outPipe->Insert(prev);
  bigq.waitUntilDone();
  dr->outPipe->ShutDown();
  printf("Exiting duplicate removal thread function\n");
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
  printf("Entered duplicate removal run\n");
  this->inPipe = &inPipe;
  this->outPipe = &outPipe;
  this->schema = &mySchema;
  this->ordermaker = new OrderMaker(&mySchema);
  this->biqOutPipe = new Pipe(100);
  this->runlen =5;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_create(&(this->workerThread), &attr, duplicateRemovalThreadFunction, (void*) this);
}
void DuplicateRemoval::WaitUntilDone (){
  pthread_join(this->workerThread, NULL);

}
void DuplicateRemoval::Use_n_Pages (int n){
  this->runlen = runlen;
}
