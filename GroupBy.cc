#include "RelOp.h"
#include "Record.h"

void* GroupBy_Thread(void * currentObject) {
  GroupBy objGroupBy = (*(GroupBy *) currentObject);
  objGroupBy.GroupByOperation();
  pthread_exit(NULL);

}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
    Function &computeMe, Schema *objsch) {
  this->gb_inputPipe = &inPipe;
  this->gb_outputPipe = &outPipe;
  this->gb_orderMaker = &groupAtts;
  this->gb_computeMe = &computeMe;
  this->objsch = objsch;
  pthread_create(&gb_workerThread, NULL, GroupBy_Thread, (void *) this);
}

void GroupBy::WaitUntilDone() {
  pthread_join(gb_workerThread, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
  this->gb_runlen = runlen;
}

void GroupBy::GroupByOperation() {
  vector<Record *> recordVectorForPipe;
  Pipe *tempPipe = new Pipe(10000000);
  Pipe *inPipeforProject = new Pipe(10000000);
  Pipe *outPipeforProject = new Pipe(10000000);
  Record *fetchMe, *temprecord, *sumRecord, *grpattrRecord;
  int intSum = 0;
  int intResult = 0;
  double dblSum = 0.0;
  double dblResult = 0.0;
  Type stype;
  Project objProject;
  int numAttsInput = 0;
  int numAttsOutput = 0;
  int flagtomakeSchemafile = 0;
  int flagfornumAttsInput = 0;
  char *name = "tempSumSchema";
  const char *src;
  //flag =1 indicates that first record is put into the recordvector
  //flag=2 indicates that record is not identical to the records in the pipe
  int flag = 0;
  ComparisonEngine comparisonEngine;
  //Sort all the reocords using bigq
  Schema *s; //Created for adding as a parameter to the BigQ method below
  BigQ bq(*(this->gb_inputPipe), *tempPipe, *(this->gb_orderMaker),
      this->gb_runlen, *s);
  fetchMe = new Record;

  while (tempPipe->Remove(fetchMe)) {
    //cout <<"num atts: "<<fetchMe->getNumofAttributes()<<endl;
    //Insert the first record in the recordvector
    if (flag == 0) {
      //cout<<"Entering first time"<<endl;
      recordVectorForPipe.push_back(fetchMe);
      flag = 1;
      //delete fetchMe;
      fetchMe = new Record();
    }

    //keep on inserting the records in the record vector till you do not get non-matching record
    else {
      Record *recordforVector;
      if (flag == 1) {
	recordforVector = new Record();
	recordforVector = recordVectorForPipe.front();
      }
      int result = comparisonEngine.Compare(recordforVector, fetchMe,
	  this->gb_orderMaker);
      if (result == 0) {
	flag = 3;
	recordVectorForPipe.push_back(fetchMe); // record equal to first in vector
      } else
	flag = 2; // record different from first record

      //flag =2 indicates that the last record fetched from pipe differs from the records in the recordVector

      //Step 1 apply function to each and every record in the vector to calculate the sum
      //Step 2 once sum is obtained than make sumrecord
      //Step 3 make a record containing only the grouping attribute
      //Step 4 merge both the  records
      //Note Schema is made only once and is closed only in the end
      if (flag == 2) {
	dblResult = 0.0;
	intResult = 0;
	while (!recordVectorForPipe.empty()) {
	  temprecord = new Record();
	  temprecord = recordVectorForPipe.back();
	  if (flagfornumAttsInput == 0) {
	    numAttsInput = temprecord->getNumofAttributes();
	    flagfornumAttsInput = 1;
	  }
	  recordVectorForPipe.pop_back();
	  stype = (this->gb_computeMe)->Apply(*temprecord, intSum,
	      dblSum);
	  if (stype == Int)
	    intResult += intSum;
	  else
	    dblResult += dblSum;
	}
	//set it back to 0 for other records to be fetched
	flagfornumAttsInput = 0;

	flagtomakeSchemafile = 1;
	ofstream fout(name);
	fout << "BEGIN" << endl;
	fout << "SUM_table" << endl;
	fout << "sum.tbl" << endl;
	fout << "SUM ";
	if (stype == Int)
	  fout << "Int" << endl;
	else
	  fout << "Double" << endl;
	fout << "END";
	fout.close();
	Schema mySchema("tempSumSchema", "SUM_table");
	string str, out;
	if (stype == Int) {
	  stringstream strResultStream;
	  strResultStream << intResult;
	  out = strResultStream.str();
	  str.append(out);
	  str.append("|");
	} else {
	  stringstream strResultStream;
	  strResultStream << dblResult;
	  out = strResultStream.str();
	  str.append(out);
	  str.append("|");
	}
	src = str.c_str();
	sumRecord = new Record();
	sumRecord->ComposeRecord(&mySchema, src);

	numAttsOutput = (this->gb_orderMaker)->getNumAtts();
	int num = 0;
	int *keepMe = (this->gb_orderMaker)->getwhichAtts(num);
	int *keepMeMerge = new int[num + 1];
	for (int i = 1, j = 0; i <= num; i++, j++) {
	  keepMeMerge[i] = keepMe[j];
	}
	keepMeMerge[0] = 0;
	//sumRecord->Print(&mySchema,1);
	Record *record = new Record();
	record->MergeRecords(sumRecord, temprecord, 1, numAttsInput,
	    keepMeMerge, (numAttsOutput + 1), 1);
	//record->Print(objsch,1);
	gb_outputPipe->Insert(record);
	delete record;
	delete sumRecord;
	recordVectorForPipe.clear();
	recordVectorForPipe.push_back(fetchMe);
	delete keepMe;
	delete keepMeMerge;
	delete recordforVector;
	flag = 1;

      }
      fetchMe = new Record;

    }
  }
  // further processing for last record
  dblResult = 0.0;
  intResult = 0;
  while (!recordVectorForPipe.empty()) {
    temprecord = new Record();
    temprecord = recordVectorForPipe.back();
    if (flagfornumAttsInput == 0) {
      numAttsInput = temprecord->getNumofAttributes();
      flagfornumAttsInput = 1;
    }
    recordVectorForPipe.pop_back();
    stype = (this->gb_computeMe)->Apply(*temprecord, intSum,
	dblSum);
    if (stype == Int)
      intResult += intSum;
    else
      dblResult += dblSum;
  }
  //set it back to 0 for other records to be fetched
  flagfornumAttsInput = 0;

  flagtomakeSchemafile = 1;
  ofstream fout(name);
  fout << "BEGIN" << endl;
  fout << "SUM_table" << endl;
  fout << "sum.tbl" << endl;
  fout << "SUM ";
  if (stype == Int)
    fout << "Int" << endl;
  else
    fout << "Double" << endl;
  fout << "END";
  fout.close();
  Schema mySchema("tempSumSchema", "SUM_table");
  string str, out;
  if (stype == Int) {
    stringstream strResultStream;
    strResultStream << intResult;
    out = strResultStream.str();
    str.append(out);
    str.append("|");
  } else {
    stringstream strResultStream;
    strResultStream << dblResult;
    out = strResultStream.str();
    str.append(out);
    str.append("|");
  }
  src = str.c_str();
  sumRecord = new Record();
  sumRecord->ComposeRecord(&mySchema, src);

  numAttsOutput = (this->gb_orderMaker)->getNumAtts();
  int num = 0;
  int *keepMe = (this->gb_orderMaker)->getwhichAtts(num);
  int *keepMeMerge = new int[num + 1];
  for (int i = 1, j = 0; i <= num; i++, j++) {
    keepMeMerge[i] = keepMe[j];
  }
  keepMeMerge[0] = 0;
  //sumRecord->Print(&mySchema,1);
  Record *record = new Record();
  record->MergeRecords(sumRecord, temprecord, 1, numAttsInput,
      keepMeMerge, (numAttsOutput + 1), 1);
  //record->Print(objsch,1);
  gb_outputPipe->Insert(record);
  delete record;
  delete sumRecord;
  recordVectorForPipe.clear();
  recordVectorForPipe.push_back(fetchMe);
  delete keepMe;
  delete keepMeMerge;
  //      delete recordforVector;
  flag = 1;

  gb_outputPipe->ShutDown();
  remove(name);
}
