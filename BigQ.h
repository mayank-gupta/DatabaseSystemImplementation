#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include<vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {
  friend class Record;
  private:
  Pipe* in;
  Pipe* out;

  OrderMaker* sortorder;
  int runlen;
  vector <int> runHeaderPositions;
  int runCount;
  ComparisonEngine ce;
  public:
  pthread_t workerThread;
  static int ID;
  int tid;
  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();

  void SORTIndividualRun(vector<Record*> &);
  int WriteSortedRunToDisk(vector<Record*> &,File*);
  void MergeRuns(File* file);
  void waitUntilDone();
  Pipe* getIn(){
    return in;
  }
  Pipe* getOut(){
    return out;
  }
  int getRunLen(){
    return runlen;
  }
  OrderMaker* getOrderMaker(){
    return sortorder;
  }
  vector<int> getRunHeaderPositions(){
    return runHeaderPositions;
  }
  void addRunHeaderPositions(int push_value){
    runHeaderPositions.push_back(push_value);
  }

  void incrementRunCount(){
    runCount++;
  }
  int getRunCount(){
    return runCount;
  }

};

#endif
