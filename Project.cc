/*
 * Project.cc
 *
 *      Author: Mayank Gupta
 */

#include "Project.h"

void* selectProjectThreadFunction(void* obj){
  printf("Entered project worker thread\n");
  Project* p = (Project*) obj;

  Record* record = new Record();
  int count = 0;
  while(p->inPipe->Remove(record)){
    record->Project(p->keepme,p->numAttsOutput,p->numAttsInput);
    cout<<++count<<endl;
    p->outPipe->Insert(record);
    record = new Record();
  }

  printf("Project worker thread finished it job\n");
  delete record;
  p->outPipe->ShutDown();
  return (NULL);

}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
  printf("Run called for project\n");
  this->inPipe = &inPipe;
  this->outPipe = &outPipe;
  this->keepme = keepMe;
  this->numAttsInput = numAttsInput;
  this->numAttsOutput = numAttsOutput;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_create(&(this->workerThread), &attr, selectProjectThreadFunction, (void*) this);
}
void Project::WaitUntilDone (){
  pthread_join(this->workerThread, NULL);
}
void Project::Use_n_Pages (int runlen){
  this->runlen = runlen;
}
