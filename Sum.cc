#include "Sum.h"

void *SumWorker(void *myData){

  Record buffer;

  Sum* selSum = (Sum*)myData;
  int intResult = 0,sumInt = 0;
  double dblResult = 0,sumDouble = 0	;
  Type result;

  //Take one record from DBFile at a time use cnf compare function to check if record satisfies it if yes put it into the output pipe

  while (selSum->myInputPipe->Remove(&buffer))
  {

    dblResult = 0;
    intResult = 0;
    //count++;
    //cout<<"the result of apply is"<<result<<endl;
    //Pass this record to function.apply
    result = selSum->toCompute->Apply(buffer,intResult,dblResult);

    sumDouble += dblResult;
    sumInt += intResult;

  }

  //cout<<"the count of records"<<count<<endl;
  Schema sumSchema(result);
  //sumSchema.CreateSumSchema(result);
  //cout<<intResult<<"and"<<dblResult<<"and"<<sum1<<endl;

  //dblResult = 13.44;
  char* sumStr = new char[10000];
  if(result == Int){
    //make record schema type with type int
    sprintf(sumStr,"%d",sumInt);
  } else if(result = Double){
    //make record schema with type double
    sprintf(sumStr,"%f",sumDouble);
  }
  char* delimiter =new char[2];
  delimiter="|";
  strcat(sumStr,delimiter);


  //cout<<"IN Select Sum:: outside while loop "<<sumStr<<endl;


  buffer.ComposeRecord(&sumSchema,sumStr);

  //buffer.Print(&sumSchema);
  selSum->myOutputPipe->Insert(&buffer);
  selSum->myOutputPipe->ShutDown();
  delete sumStr;
  pthread_exit(NULL);
}

void Sum::Run(Pipe &inPipe,Pipe &outPipe, Function &computMe){

  myInputPipe = &inPipe;
  myOutputPipe = &outPipe;
  toCompute = &computMe;
  pthread_create(&thread,NULL,SumWorker,(void*)this);
}

void Sum::WaitUntilDone(){
  pthread_join(thread,NULL);
}

void Sum::Use_n_Pages(int runLen){

}
