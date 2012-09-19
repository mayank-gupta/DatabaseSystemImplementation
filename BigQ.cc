#include "BigQ.h"
#include<algorithm>
#include<queue>
#include<functional>
#include <ctime>
#include <cstdlib>
#include <stdio.h>

int BigQ::ID=0;
void printPage(Page* page){
  Schema mySchema ("catalog", "lineitem");
  Record* temp = new Record();
  while(page->GetFirst(temp)){
    temp->Print(&mySchema);
  }
  printf("\nFinished printing the page\n");
  delete temp;
  return;
}


class SortRecord{
  private:
    OrderMaker* sortOrder;
    ComparisonEngine ce;
  public:
    SortRecord(OrderMaker* sortOrder){
      this->sortOrder = sortOrder;
    }
    bool operator()(Record *left, Record *right){

      return ce.Compare(left,right,sortOrder) < 0;
    }
};
void BigQ::SORTIndividualRun(vector<Record*> &records){
  Schema mySchema("catalog", "lineitem");

  printf("Entered sort individual run \n");



  sort(records.begin(),records.end(),SortRecord(sortorder));
  printf("Finished sorting individual run \n");



}

int BigQ::WriteSortedRunToDisk(vector<Record*> &records,File* file){
  Page* page = new Page();

  //see if you can open it once and close it once in worker thread function


  for(int i=0;i<records.size();i++){
    if(page->Append(records[i])==0){
      if(file->GetLength()==0){
	file->AddPage(page,0);
      }else
	file->AddPage(page,file->GetLength()-1);

      page->EmptyItOut();
      page->Append(records[i]);
    }
  }
  if(file->GetLength()==0){
    file->AddPage(page,0);
  }else
    file->AddPage(page,file->GetLength()-1);

  //printf("Number of pages in file is %d\n",file->GetLength()-1);
  return file->GetLength()-1;

}


class MyRecord{
  public:
    Record* record;
    int run;
    MyRecord(){}
    MyRecord(Record* record,int run){
      this->record = record;
      this->run = run;
    }
};


OrderMaker* globalSortOrder;

class MyComparator {
  public:

    ComparisonEngine ce;


    bool operator()(MyRecord *left, MyRecord *right){
      return ce.Compare(left->record,right->record,globalSortOrder) >= 0;
    }
};

void BigQ::MergeRuns(File* file){

  printf("Entered Merge Runs\n");
  //populate pages and then records

  vector<int> currentHeaderPositions(runHeaderPositions);
  Page* page = new Page[runCount];


  //populate pages
  //printf("populating pages\n");
  for(int i=0;i<runCount;i++){
    file->GetPage(&page[i],currentHeaderPositions[i]);
    currentHeaderPositions[i]++;
  }
  //printf("Done...\n");

  //populate records
  //printf("populating records\n");

  globalSortOrder = sortorder;
  priority_queue <MyRecord*,vector<MyRecord*>,MyComparator> pq;
  Record* record = new Record();
  for(int i=0;i<runCount;i++){
    page[i].GetFirst(record);
    pq.push(new MyRecord(record,i));
    record = new Record();
  }
  //printf("Done..");

  printf("Merging Runs...\n");
  Schema mySchema("catalog", "supplier");
  MyRecord* top;
  while(!pq.empty()){
    top = pq.top();
    //top->record->Print(&mySchema);
    out->Insert(top->record);

    pq.pop();
    Record* record = new Record();
    int currentRun = top->run;
    if(page[currentRun].GetFirst(record)){
      pq.push(new MyRecord(record,currentRun));
    }else{
      if(currentHeaderPositions[currentRun]<runHeaderPositions[currentRun+1]){
	file->GetPage(&page[currentRun],currentHeaderPositions[currentRun]);
	currentHeaderPositions[currentRun]++;
	if(page[currentRun].GetFirst(record)){
	  pq.push(new MyRecord(record,currentRun));
	}else{
	  printf("Error: while getting record from the recently loaded page\n");
	}
      }
    }
  }

  delete record;
  printf("Done Merging......\n");


}
void* startSorting(void* obj){
  File* file = new File();
  BigQ* bigQ = (BigQ*) obj;
  srand(time(0));
  int x = bigQ->tid;
  printf("\n\n%d\n",x);
  char meta[10];
  sprintf(meta,"%d",x);
  file->Open(0,meta);
  file->Close();
  file->Open(1,meta);
  Schema mySchema("catalog", "supplier");

  printf("BigQ Worker Thread Starting\n");
  Record* record = new Record(); // CHeck if you delete this after using


  long maxNumOfRecordsInaRun = (PAGE_SIZE*((bigQ->getRunLen())));
  long sum=0;
  int numOfRecordsInRun =0;
  vector <Record*> records;
  records.reserve(maxNumOfRecordsInaRun);
  bigQ->addRunHeaderPositions(0);

  while(bigQ->getIn()->Remove(record)){
    sum+= ((int *) record->GetBits())[0];
    //record->Print(&mySchema);
    if(sum>=maxNumOfRecordsInaRun){
      bigQ->incrementRunCount();
      //printf("Finished Loading Run# %d, # of records= %d\n",bigQ->getRunCount(),records.size());
      //printf("Preparing to sort the run\n");
      bigQ->SORTIndividualRun(records);
      bigQ->addRunHeaderPositions(bigQ->WriteSortedRunToDisk(records,file));
      sum =0;
      numOfRecordsInRun =0;
      records.clear();

    }
    records.push_back(record);
    numOfRecordsInRun++;
    record = new Record();

  }
  bigQ->incrementRunCount();
  //printf("Finished Loading Run# %d, # of records= %d\n",bigQ->getRunCount(),records.size());
  //printf("Preparing to sort the run\n");
  bigQ->SORTIndividualRun(records);
  bigQ->addRunHeaderPositions(bigQ->WriteSortedRunToDisk(records,file));
  records.clear();

  //start merging together runs now. Have to implement a priority queue
  bigQ->MergeRuns(file);
  delete record;
  file->Close();
  delete file;
  bigQ->getOut()->ShutDown();
  if( remove( meta ) != 0 )
    perror( "Error deleting file" );
  else
    puts( "File successfully deleted" );
  return 0;
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {


  this->in = &in;
  this->out = &out;
  this->sortorder = &sortorder;
  this->runlen = runlen;
  this->runCount =0;
  BigQ::ID++;
  this->tid = BigQ::ID;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_create(&workerThread,&attr,startSorting,(void* )this);
  //this->ID = pthread_self();
  //pthread_join (workerThread, NULL);

}

void BigQ::waitUntilDone(){
  pthread_join (workerThread, NULL);
}
BigQ::~BigQ () {

}
