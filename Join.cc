/*
 * Join.cc
 *
 *      Author: Mayank Gupta
 */

#include "Join.h"
#include <vector>
#include <string.h>
#include <assert.h>

void printVector(vector<Record*> arr){
  Schema mySchema("catalog", "supplier");
  for(int i=0;i<arr.size();i++){
    arr[i]->Print(&mySchema);
  }
}

static inline int get_num_attrs(Record *rec){
  return  (((int*) (rec->GetBits()))[1]/sizeof(int) - 1 );
}

static inline int get_attr_offset_from_data_chunk(Record *rec, int attr){
  int num_attrs = get_num_attrs(rec);
  assert(attr <= num_attrs);
#define INT_ARR ((int*)rec->GetBits())
  return (INT_ARR[attr] - INT_ARR[1]) ;
}
#undef INT_ARR

static inline Record*  concatenate_records(Record *left,Record *right){

  //debug info
  /*
     Attribute IA = {"int", Int};
     Attribute SA = {"string", String};
     Attribute DA = {"double", Double};
     int outAtts = 12;
     Attribute s_nationkey = {"s_nationkey", Int};
     Attribute ps_supplycost = {"ps_supplycost", Double};
     Attribute joinatt[] = {IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
     Schema join_sch ("join_sch", outAtts, joinatt);
     */
  //debug info end


  Record *ret_rec = new Record;
  //NOT_NULL(ret_rec);
  int size_left,size_right;
  size_left = ((int*) (left->GetBits()))[0];
  size_right = ((int*) (right->GetBits()))[0];
  int num_attrs_left = get_num_attrs(left);
  int num_attrs_right = get_num_attrs(right);
  char buffer[PAGE_SIZE];
  memset(buffer,'\0',PAGE_SIZE);
  int start_pos = (num_attrs_right + num_attrs_left + 1)*sizeof(int);
  /*
     Schema debug_left("catalog","supplier");
     Schema debug_right("catalog","partsupp");
     fprintf(stdout,"\nLeft_rec\n");
     print_rec_struct(left);
     left->Print(&debug_left);
     fprintf(stdout,"\nright_rec\n");
     print_rec_struct(right);
     right->Print(&debug_right);
     Schema debug_test("catalog","test");
     */
  //copy left record
  int len;
  //copy contents of first record
#define LBITS (left->GetBits())
#define RBITS (right->GetBits())
  memcpy(buffer + start_pos ,LBITS + ((int *)LBITS)[1] , ((int *)LBITS)[0] - ((int *)LBITS)[1]);
  start_pos += ((int *)LBITS)[0] - ((int *)LBITS)[1];
  memcpy(buffer + start_pos ,RBITS + ((int *)RBITS)[1] , ((int *)RBITS)[0] - ((int *)RBITS)[1]);
  //fix indicies

  int i =  1;
  int insert_pos = 0;
  start_pos = (num_attrs_right + num_attrs_left + 1)*sizeof(int) ;
  while( i <= num_attrs_left){
    insert_pos = start_pos + get_attr_offset_from_data_chunk(left, i);
    memcpy(buffer + ( i )*sizeof(int),(char*)&insert_pos,sizeof(int));
    i++;
  }
  //now loop over the right record and fix offset
  start_pos = (num_attrs_right + num_attrs_left + 1)*sizeof(int) +  ((int *)LBITS)[0] - ((int *)LBITS)[1];
  insert_pos = 0;
  i = 1;
  while( i <= num_attrs_right){
    insert_pos = start_pos + get_attr_offset_from_data_chunk(right, i);
    memcpy(buffer + (num_attrs_left + i)*sizeof(int),(char*)&insert_pos,sizeof(int));
    i++;
  }

  start_pos = ((int*)LBITS)[0] + ((int*)RBITS)[0] - sizeof(int);
  //finally write out the size
  memcpy(buffer,(char *)&start_pos,sizeof(int));
  //print_rec_buffer(buffer);
  //make bits in return record
  ret_rec->CopyBits(buffer,((int*)buffer)[0]);
  //ret_rec->Print(&join_sch);
  return ret_rec;
}
#undef LBITS
#undef RBITS


void mergeAndClearVectors(vector<Record*> &left,vector<Record*> &right,Pipe* outPipe){
  //printf("Started merging records from left and right vectors\n");
  for(int i=0;i<left.size();i++){
    for(int j=0;j<right.size();j++){
      Record* mergedRecord = concatenate_records(left[i],right[j]);
      outPipe->Insert(mergedRecord);
    }
  }

  left.clear();
  right.clear();
  //printf("Done merging records from left and right vectors\n");
}


void* joinThreadFunction(void* obj){
  printf("Entered join thread function\n");


  Join* joinObj = (Join*) obj;


  BigQ bqLeft(*(joinObj->inPipeLeft),*(joinObj->bigqOutPipeLeft),*(joinObj->orderMakerLeft),joinObj->runlen);




  BigQ bqRight(*(joinObj->inPipeRight),*(joinObj->bigqOutPipeRight),*(joinObj->orderMakerRight),joinObj->runlen);

  Record* left = new Record();
  Record* right = new Record();
  Record* tempLeft;
  ComparisonEngine ce;
  vector<Record*> leftVector;
  vector<Record*> rightVector;

  joinObj->bigqOutPipeLeft->Remove(left);
  joinObj->bigqOutPipeRight->Remove(right);


  bool leftEmpty=false,rightEmpty=false;

  Schema mySchema("catalog", "partsupp");
  Schema supplierSchema("catalog","supplier");

  while(1){

    leftVector.push_back(left);
    tempLeft = new Record();
    if(!(joinObj->bigqOutPipeLeft->Remove(tempLeft))){
      leftEmpty = true;
    }
    while(leftEmpty==false && ce.Compare(left,tempLeft,joinObj->orderMakerLeft)==0){
      leftVector.push_back(tempLeft);
      left = tempLeft;
      tempLeft = new Record();
      if(!(joinObj->bigqOutPipeLeft->Remove(tempLeft))){
	leftEmpty = true;
      }
    }
    left = tempLeft;
    // till now I have filled all the equal guyz in left into left vector

    Record* firstGuyFromLeftVector=NULL;


    if(leftVector.size()>0){
      firstGuyFromLeftVector = leftVector[0];
    }
    else
      printf("ERROR !!! Trying to get from an empty vector\n");

    //debug
    //printVector(leftVector);
    //right->Print(&mySchema);
    //firstGuyFromLeftVector->Print(&supplierSchema);

    //check if right one is greater than first guy from left vector, if yes clear the vector
    if(ce.Compare(firstGuyFromLeftVector,joinObj->orderMakerLeft,right,joinObj->orderMakerRight)<0){
      leftVector.clear();
    }
    // else if left guy is greater, then keep iterating right, till one is equal or greater than left
    else if(ce.Compare(firstGuyFromLeftVector,joinObj->orderMakerLeft,right,joinObj->orderMakerRight)>0){
      while(ce.Compare(firstGuyFromLeftVector,joinObj->orderMakerLeft,right,joinObj->orderMakerRight)>0){
	if(!(joinObj->bigqOutPipeRight->Remove(right))){
	  rightEmpty = true;
	  break;
	}
      }
      //now either right is greater or it is equal

      // if it is greater, then no match was found, clear the left vector
      if(ce.Compare(firstGuyFromLeftVector,joinObj->orderMakerLeft,right,joinObj->orderMakerRight)<0){
	leftVector.clear();
      }

      //otherwise, right = firstGuyFromLeftVector
      else {
	while(ce.Compare(firstGuyFromLeftVector,joinObj->orderMakerLeft,right,joinObj->orderMakerRight)==0){
	  rightVector.push_back(right);
	  right = new Record();
	  if(!(joinObj->bigqOutPipeRight->Remove(right))){
	    rightEmpty = true;
	    break;
	  }
	}
	//at this point right is definitely greater than first guy from left vector
	// so now it is time to merge the vectors together
	if(rightVector.size()>0)
	  mergeAndClearVectors(leftVector,rightVector,joinObj->outPipe);
      }
    }
    // else right = first guy from left vector
    else {
      while(ce.Compare(firstGuyFromLeftVector,joinObj->orderMakerLeft,right,joinObj->orderMakerRight)==0){
	rightVector.push_back(right);
	right = new Record();
	if(!(joinObj->bigqOutPipeRight->Remove(right))){
	  rightEmpty = true;
	  break;
	}
      }
      //now merge
      if(rightVector.size()>0)
	mergeAndClearVectors(leftVector,rightVector,joinObj->outPipe);
    }

    // at this point either one or both of the pipes is(are) empty
    // or we have to deal with one left and one right, as in the beginning
    //hence we can revert back to our loop
    if(leftEmpty || rightEmpty)
      break;


  }

  //printVector(leftVector);




  joinObj->outPipe->ShutDown();
  printf("Exiting join thread function\n");
  return (NULL);
}


void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
  this->inPipeLeft = &inPipeL;
  this->inPipeRight = &inPipeR;
  this->outPipe = &outPipe;
  this->cnf = &selOp;
  this->literal = &literal;
  this->bigqOutPipeLeft = new Pipe(100);
  this->bigqOutPipeRight = new Pipe(100);
  this->orderMakerLeft = new OrderMaker();
  this->orderMakerRight = new OrderMaker();
  if(this->cnf->GetSortOrders(*(this->orderMakerLeft),*(this->orderMakerRight)) ==0){
    // make another thread and call its function, or call some  other function
  }
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_create(&workerThread, &attr, joinThreadFunction, (void*) this);
}
void Join::WaitUntilDone (){
  pthread_join(workerThread, NULL);
}
void Join::Use_n_Pages (int n){
  this->runlen = runlen;
}
