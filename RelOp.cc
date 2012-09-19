#include "RelOp.h"

typedef struct groupstruct
{
  Pipe *ipipe;
  Pipe *opipe;
  OrderMaker *group;
  Function *comp;
  int runlen;
};

void* startgroupby_thread(void *ptr)
{
  groupstruct* group=(groupstruct*) ptr;
  Pipe *bigout=new Pipe(100);
  BigQ bigq (*group->ipipe, *bigout, *group->group, group->runlen);
  Record *grp1=new Record();
  Record *grp2=new Record();
  Record *insert=new Record();
  int iresult=0;
  double dresult=0.0;
  int intresult=0;
  double doubleresult=0.0;
  int count=0;

  int rightatts=0;
  int totalatts;
  Record *grouprec=new Record();


  string out;
  ofstream fout("sum");
  fout<<"BEGIN"<<endl;
  fout<<"sum_table"<<endl;
  fout<<"sum.tbl"<<endl;
  fout<<"SUM ";

  bigout->Remove(grp1);
  int rettype=group->comp->Apply(*grp1,intresult,doubleresult);
  if(rettype==Int)
  {
    iresult+=intresult;
    fout<<"Int"<<endl;
    fout<<"END"<<endl;
  }
  else
  {
    dresult+=doubleresult;
    fout<<"Double"<<endl;
    fout<<"END"<<endl;
  }
  fout.close();


  ComparisonEngine ceng;
  int count1=0;
  while(bigout->Remove(grp2))
  {
    int compare=ceng.Compare(grp1,grp2,group->group);

    if(compare==0)
    {
      rettype=group->comp->Apply(*grp2,intresult,doubleresult);
      if(rettype==Int)
      {
	iresult+=intresult;
      }
      else
      {

	dresult+=doubleresult;

      }
    }
    else
    {

      string str;
      if(rettype==Int)
      {


	stringstream ostream;
	ostream<<iresult;
	out=ostream.str();
	str.append(out);
	str.append("|");
	iresult=0;
      }
      else
      {

	stringstream ostream;
	count++;
	ostream<<dresult;
	out=ostream.str();
	str.append(out);
	str.append("|");
	dresult=0;
      }

      Schema mySchema("sum","sum_table");

      rightatts=grp1->GetNumAtts();
      totalatts=group->group->GetNumAtts()+1;
      int attstokeep[totalatts];
      attstokeep[0]=0;
      for(int i=0;i<group->group->GetNumAtts();i++)
      {
	attstokeep[i+1]=i;

      }
      const char* src=str.c_str();
      insert->ComposeRecord(&mySchema,src);
      grouprec->MergeRecords(insert,grp1,1,rightatts,attstokeep,totalatts,1);
      group->opipe->Insert(grouprec);
    }
    grp1->Copy(grp2);


  }


  rettype=group->comp->Apply(*grp2,intresult,doubleresult);
  if(rettype==Int)
  {
    iresult+=intresult;
  }
  else
  {
    dresult+=doubleresult;

  }
  string str;
  if(rettype==Int)
  {

    stringstream ostream;
    ostream<<iresult;
    out=ostream.str();
    str.append(out);
    str.append("|");
    iresult=0;
  }
  else
  {

    stringstream ostream;
    count++;
    ostream<<dresult;

    out=ostream.str();

    str.append(out);
    str.append("|");
    dresult=0;
  }
  Schema mySchema("sum","sum_table");
  rightatts=grp2->GetNumAtts();

  totalatts=group->group->GetNumAtts()+1;
  int attstokeep[totalatts];
  attstokeep[0]=0;
  for(int i=0;i<group->group->GetNumAtts();i++)
  {
    attstokeep[i+1]=i;

  }


  const char* src=str.c_str();
  insert->ComposeRecord(&mySchema,src);

  int leftatts=1;
  grouprec->MergeRecords(insert,grp2,leftatts,rightatts,attstokeep,totalatts,1);

  group->opipe->Insert(grouprec);

  group->opipe->ShutDown();

}
void GroupBy::Run(Pipe& inPipe, Pipe& outPipe, OrderMaker& groupAtts, Function& computeMe)
{
  groupstruct *groupby=new groupstruct();
  groupby->ipipe=new Pipe(100);
  groupby->ipipe=&inPipe;
  groupby->opipe=new Pipe(100);
  groupby->opipe=&outPipe;
  groupby->group=new OrderMaker();
  groupby->group=&groupAtts;
  groupby->comp=new Function();
  groupby->comp=&computeMe;
  groupby->runlen=n;
  int rc=pthread_create(&groupby_thread,NULL, startgroupby_thread,(void*)groupby);
  if(rc)
    exit(1);

}

void GroupBy::Use_n_Pages(int runlen)
{
  n=runlen;
}

void GroupBy::WaitUntilDone()
{
  pthread_join(groupby_thread,NULL);
}
