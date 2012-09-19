/*
 * SelectFile.cc
 *
 *      Author: Mayank Gupta
yank Gupta/g
 */

#include "SelectFile.h"

void* selectFileThreadFunction(void* obj) {
  fflush(stdout);
  printf("Thread Select File started\n");
  Schema mySchema("catalog", "lineitem");
  ComparisonEngine ce;
  SelectFile* sf = (SelectFile*) obj;
  Record* record = new Record();
  while (sf->dbFile->GetNext(*record)) {
    //record->Print(&mySchema);
    if(sf->cnf!=NULL){
      if (ce.Compare(record, sf->literal, sf->cnf) == 1) {
	//printf("Matched");
	//record->Print(&mySchema);
	sf->outPipe->Insert(record);
	record = new Record();
      }
    }else{
      //record->Print(&mySchema);
      sf->outPipe->Insert(record);
      record = new Record();
    }
  }

  printf("select file worker thread finished its job\n");
  sf->outPipe->ShutDown();

  delete record;
  return (NULL);

}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
  printf("Run called for select file \n");
  this->dbFile = &inFile;
  this->outPipe = &outPipe;
  this->literal = &literal;
  this->cnf = &selOp;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_create(&(this->workerThread), &attr, selectFileThreadFunction, (void*) this);
}

void SelectFile::WaitUntilDone() {
  pthread_join(this->workerThread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {
  this->runlen = runlen;
}

