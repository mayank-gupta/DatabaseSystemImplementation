/*
 * WriteOut.cc
 *
 *      Author: Mayank Gupta
 */

#include "WriteOut.h"

void* writeOutThreadFunction(void* obj){
  printf("Entered write out thread function\n");
  WriteOut* writeout = (WriteOut*) obj;
  Record record;
  while(writeout->inPipe->Remove(&record)){
    record.Print(writeout->schema,writeout->outFile);
  }

  writeout->inPipe->ShutDown();
  printf("Exiting write out thread function\n");
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
  this->inPipe = &inPipe;
  this->outFile = outFile;
  this->schema = &mySchema;
  this->runlen = 1;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_create(&workerThread, &attr, writeOutThreadFunction, (void*) this);
}
void WriteOut::WaitUntilDone () {
  pthread_join(workerThread, NULL);
}
void WriteOut::Use_n_Pages (int n) {
  this->runlen = runlen;
}
