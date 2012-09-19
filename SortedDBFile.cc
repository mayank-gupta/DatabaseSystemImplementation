#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <fstream>
// stub file .. replace it with your own SortedDBFile.cc

typedef struct {
  OrderMaker *o;
  int runlength;

} s;

SortedDBFile::SortedDBFile () {
  file = new File();
  getPage = new Page();

}

void SortedDBFile::print_file(char* name){
  //fflush(stdout);
  //getchar();

  Page page;
  int pageptr=0;
  int numrecs=0;
  Record *record = new Record();
  Schema mySchema ("catalog",name);
  if(file->GetLength()==0)
    return;

  while(pageptr<file->GetLength()-1){
    file->GetPage(&page,pageptr++);

    while(page.GetFirst(record)){
      record->Print(&mySchema);
      numrecs++;
      record = new Record();
    }
  }
  //printf("Number of records in the file = %d",numrecs);
}
void SortedDBFile::write_orderMaker_to_disk(){
  std::ofstream out;
  char meta[100];
  sprintf(meta,"meta//%s.metadata",f_path);
  out.open(meta);

  out<<type<<endl;
  out<<runLength<<endl;
  char orderMakerString[100];
  sprintf(orderMakerString,"%d\n", orderMaker->numAtts);
  out<<orderMakerString;
  for (int i = 0; i < orderMaker->numAtts; i++)
  {
    sprintf(orderMakerString,"%d\n%d\n%d\n", i, orderMaker->whichAtts[i],orderMaker->whichTypes[i]);
    out<<orderMakerString;
  }
  out.close();
}

void SortedDBFile::create_orderMaker_from_disk(){
  orderMaker = new OrderMaker();
  char meta[100];
  sprintf(meta,"meta//%s.metadata",f_path);
  std::ifstream in;
  in.open(meta);

  if(in.is_open()){

    int f_type;
    in>>f_type;
    type = (fType) f_type;
    in>>runLength;
    int n,index,whichAtts;
    int whichType;
    in>>n;
    orderMaker->numAtts = n;
    for (int i = 0; i < n; i++) {
      in>>index;
      in>>whichAtts;
      in>>whichType;
      orderMaker->whichAtts[index]=whichAtts;
      orderMaker->whichTypes[index]=(Type)whichType;
    }
  }

  in.close();
}
int SortedDBFile::Create (char *f_path, fType f_type, void *startup) {

  input = new Pipe(100);
  output = new Pipe(100);


  file->Open(0,f_path);
  s *startUp = (s*) startup;
  this->orderMaker = startUp->o;
  this->type = f_type;
  this->runLength = startUp->runlength;
  this->f_path = f_path;
  write_orderMaker_to_disk();
  mode = reading;
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
  getnext_called_in_succession = false;
  if(mode==reading){
    readingToWriting();
  }

  FILE *tableFile = fopen(loadpath, "r");
  if (tableFile == NULL) {
    //printf("Could not open the file to read \n");
  }
  Record *record = new Record();

  while (record->SuckNextRecord(&f_schema, tableFile)) {
    Add(*record);
    record = new Record();
  }
}

int SortedDBFile::Open (char *f_path) {

  input = new Pipe(100);
  output = new Pipe(100);


  this->f_path = f_path;
  file->Open(1,f_path);
  create_orderMaker_from_disk();

  //printf("Run length = %d\n",runLength);
  mode = reading;
  pageToPick =0;
  numRecsInOriginalGetPage = 0;
  getnext_called_in_succession = false;


}



int SortedDBFile::Close () {

  getnext_called_in_succession = false;
  if(mode==writing){
    writingToReading();
  }

  file->Close();
  //delete literal_queryOrderMaker;
  //delete query_ordermaker;
  delete input;
  delete output;
  delete file;
  delete getPage;
  //delete orderMaker;

}

void SortedDBFile::readingToWriting(){
  mode = writing;
  bigq = new BigQ(*input,*output,*orderMaker,runLength);
}

void SortedDBFile::writingToReading(){
  mode=reading;
  input->ShutDown();
  Record* record = new Record();
  Record* recordFromFile = new Record();
  File* newFile = new File();
  Page *page = new Page();
  Page *newPage = new Page();
  int pageOffset =0;
  char fileName[100];
  sprintf(fileName,"new%s",this->f_path);
  newFile->Open(0,fileName);

  if(file->curLength>0){
    int numrecs=0;
    file->GetPage(page,pageOffset++);
    page->GetFirst(recordFromFile);

    while(output->Remove(record)){
      numrecs++;
      while(recordFromFile->GetBits()!=NULL && ce.Compare(recordFromFile,record,orderMaker)<=0){
	if(newPage->Append(recordFromFile)==0){
	  if(newFile->curLength==0){
	    newFile->AddPage(newPage,0);
	  }else
	    newFile->AddPage(newPage,newFile->curLength-1);
	  newPage->EmptyItOut();
	  newPage->Append(recordFromFile);
	}
	recordFromFile = new Record();
	if(page->GetFirst(recordFromFile)==0){
	  if(pageOffset<file->curLength-1){
	    file->GetPage(page,pageOffset++);
	    page->GetFirst(recordFromFile);
	  }else
	    recordFromFile->SetBits(NULL);

	}
      }

      //what about the remaining records in the last page?

      //record from bigQ is smaller than record from sorted file

      if(newPage->Append(record)==0){
	if(newFile->curLength==0){
	  newFile->AddPage(newPage,0);
	}else
	  newFile->AddPage(newPage,newFile->curLength-1);
	newPage->EmptyItOut();
	newPage->Append(record);
      }
      record = new Record();
    }

    // one recordinfile to be pushed and then others in the old sorted file

    while(recordFromFile->GetBits()!=NULL){
      if(newPage->Append(recordFromFile)==0){
	if(newFile->curLength==0){
	  newFile->AddPage(newPage,0);
	}else
	  newFile->AddPage(newPage,newFile->curLength-1);
	newPage->EmptyItOut();
	newPage->Append(recordFromFile);
      }
      recordFromFile = new Record();
      if(page->GetFirst(recordFromFile)==0){
	if(pageOffset<file->curLength-1){
	  file->GetPage(page,pageOffset++);
	  page->GetFirst(recordFromFile);
	}else
	  recordFromFile->SetBits(NULL);

      }
    }

    if(newFile->curLength==0){
      newFile->AddPage(newPage,0);
    }else
      newFile->AddPage(newPage,newFile->curLength-1);
    newPage->EmptyItOut();

    //delete the old sorted file
    if(remove(f_path)==-1){
      //printf("Error Removing the old file\n");
    }

    if(!rename(fileName,f_path)==-1){
      //printf("Error Renaming the file\n");
    }
    file = newFile;

    //printf("Number of records removed from output pipe = %d\n",numrecs);

  }else{

    //the case when sorted file is empty
    // in this case push all the records from pipe into the file
    int numrecs=0;
    while(output->Remove(record)){
      numrecs++;
      if(page->Append(record)==0){
	////printf("Adding a page to the file...\n");
	if(file->curLength==0){
	  file->AddPage(page,0);
	}else
	  file->AddPage(page,file->curLength-1);
	page->EmptyItOut();
	page->Append(record);
      }
      record = new Record();
    }


    ////printf("Adding a page to the file...\n");
    if(file->curLength==0){
      file->AddPage(page,0);
    }else
      file->AddPage(page,file->curLength-1);
    page->EmptyItOut();
    //printf("Number of records removed from output pipe = %d\n",numrecs);

  }
  output->ShutDown();

  if(pageToPick>0){
    Record record;
    //now update the getpage from disk

    int num_recs_in_current_get_page = getPage->numRecs;
    //printf("pages in original = %d\n",numRecsInOriginalGetPage);
    //printf("pages in current = %d\n",num_recs_in_current_get_page);
    file->GetPage(getPage,pageToPick-1);
    int num_recs_in_new_get_page = getPage->numRecs;

    //printf("pages in new = %d\n",num_recs_in_new_get_page);
    if(num_recs_in_new_get_page>numRecsInOriginalGetPage){
      for(int i=0;i<(numRecsInOriginalGetPage-num_recs_in_current_get_page);i++){
	//printf("removing stale records..\n");
	getPage->GetFirst(&record);
      }
      numRecsInOriginalGetPage = num_recs_in_new_get_page;
    }

  }
  delete page;
  delete newPage;
  delete record;
  //empty the bigq instance HOW???
}
void SortedDBFile::Add (Record &rec) {
  if(getnext_called_in_succession)
    getnext_called_in_succession = false;
  input->Insert(&rec);
  if(mode==reading){
    readingToWriting();
  }


}

void SortedDBFile::MoveFirst() {
  getnext_called_in_succession = false;
  pageToPick = 0;
  getPage->EmptyItOut();

}

int SortedDBFile::GetNext(Record &fetchme) {

  if(mode==writing){
    writingToReading();
  }

  if (file->GetLength() == 0) {
    return 0;
  }

  if (getPage->GetFirst(&fetchme) == 0) {
    if (pageToPick > (file->GetLength() - 2)) {
      return 0;
    }else{
      file->GetPage(getPage, pageToPick);
      //printf("got page %d..\n",pageToPick);
      pageToPick++;
      numRecsInOriginalGetPage = getPage->numRecs;
      getPage->GetFirst(&fetchme);
    }

  }

  return 1;
}


void SortedDBFile::restore_getPage(int numRecsinGetpage,int temp_page_to_pick){
  pageToPick = temp_page_to_pick;
  //recreating the get page

  Record temp;
  file->GetPage(getPage,pageToPick-1);
  int till = getPage->numRecs - numRecsinGetpage;

  for(int i=0;i<(till);i++){

    getPage->GetFirst(&temp);
  }

}


// what if I land bang on...hit the very first record of a page and it matches the query ordermaker
// but the records prior to that might also match ...say l_quantity = 36..matches for page 69's first
// record, but what if page 68 had some records tht also have l_quantity as 36...? I would never ever
// check them as I only check the records which come after the one which has matched the query ordermaker.

// binary search not perfect...think of a work around..
// goal should be to try and match the first record in the sorted file which matches the query ordermaker.
int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  Schema mySchema("catalog", "lineitem");
  if(mode==writing){
    writingToReading();
  }
  //stuff for restoring the pointer if getnext fails
  int numRecsinGetpage = getPage->numRecs;
  int temp_page_to_pick = pageToPick;


  //construct a new query ordermaker
  //see how to delete this, not clear yet
  if(getnext_called_in_succession==false){


    query_ordermaker = new OrderMaker();
    //make sure you delete this
    literal_queryOrderMaker = new OrderMaker();
    cnf.get_query_ordermaker(query_ordermaker,literal_queryOrderMaker,orderMaker);
    query_ordermaker->Print();
    literal_queryOrderMaker->Print();
    //getchar();
    //fflush(stdout);
    getnext_called_in_succession = true;




    if(query_ordermaker->numAtts==0){
      Record *record = new Record();
      while(GetNext(*record)){
	if(ce.Compare(record,&literal,&cnf)){
	  fetchme.Consume(record);
	  return 1;
	}
      }
      restore_getPage(numRecsinGetpage,temp_page_to_pick);
      //printf("No more records...\n");
      return 0;
    }else{

      //printf("number of pages in the file is %d\n",file->GetLength()-2);
      int low,middle,high;
      if(pageToPick>0){
	low = pageToPick-1;
      }else low = 0;

      high = file->GetLength()-2;
      Page* page = new Page();
      Record* record = new Record();
      bool match = false;
      while(low<=high){
	middle = (low+high)/2;
	//printf("low = %d, high = %d, Middle = %d\n",low,high,middle);
	file->GetPage(page,middle);
	//if middle is the current getpage, then it should not look at record before the current record pointer
	if(middle==pageToPick-1){
	  page = getPage;
	}
	//check if literal is smaller than the first record of the page, if so
	//set high = middle -1
	if(!page->GetFirst(record)){
	  middle++;
	  if(middle<file->GetLength()-1){
	    file->GetPage(page,middle);
	    page->GetFirst(record);
	  }
	  else{
	    //printf("Out of pages..no more records..\n");
	    restore_getPage(numRecsinGetpage,temp_page_to_pick);
	    return 0;
	  }

	}

	//record->Print(&mySchema);
	//query_ordermaker->Print();
	if(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)==0){
	  //printf("Bang on match..v first record of the page..\n");
	  //get the first record of the last page in an effort to find the very first record
	  // that satisfies the equality above.

	  // check if this is what we r looking for


	  // no? so go to the first record from where this predicate started being true...and then
	  //scan forward till you find a match.
	  do{
	    --middle;
	    if(middle<temp_page_to_pick-1){
	      match=true;
	      middle++;
	      break;
	    }else if(middle==temp_page_to_pick-1){
	      page = getPage;
	    }else
	      file->GetPage(page,middle);

	    page->GetFirst(record);
	  }while(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)==0);

	  //now we are one page behind the page whose firs t record matches the literal
	  // in terms of query ordermaker..so we go on and check this page to find out
	  //the very first record which is equal to the literal in terms of the query maker.
	  while(match==false && page->GetFirst(record)){
	    if(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)==0){
	      //printf("\nFound a record one page before the current middle...\n");
	      //record->Print(&mySchema);
	      match = true;
	      break;
	    }
	  }
	  // did not find any record in the page before the page whose first record is good.
	  //so we will just start with the first record of the next page, which is guaranteed
	  //to be good...so remove the if condition below after testing.
	  if(match==false){
	    middle++;
	    file->GetPage(page,middle);
	    page->GetFirst(record);
	    //remove the line below later as this condition should always hold
	    if(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)==0){
	      //printf("Found an expected match..delete this comparison..\n");
	      //record->Print(&mySchema);
	      break;
	    }
	  }

	}else if(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)>0){
	  //it is always coming here meaning, the comparison is always showing record to be greater
	  //than the literal..
	  high = middle-1;
	  continue;
	}else{

	  //check all
	  while(page->GetFirst(record)){

	    if(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)==0){
	      // what to do if the middle page comes out to be the current page and then
	      // how do i remove all the records to bring the page in tune with the getpage
	      //printf("\n\nFound a  match !!!!\n\n");


	      //record->Print(&mySchema);
	      match=true;
	      break;
	    }else if(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)>0){
	      //earlier we made sure that the first record was smaller than literal, so
	      //we proceeded, but now it says that current record is greater than the
	      //literal, impossible..so no record in the file is going to match this literal
	      //do cleanup and return because if the record was there it had to be in betweeen this
	      //and this and the first page
	      restore_getPage(numRecsinGetpage,temp_page_to_pick);
	      //printf("did not match any record from inside while, soiry..\n");
	      return 0;
	    }

	  }
	}
	if(match){
	  //got the record, it is in Record
	  break;
	}
	//else start searching in the next half
	low = middle+1;

	////printf("after one round..\n");
	//printf("low = %d, high = %d, Middle = %d\n",low,high,middle);


      }
      if(match){
	//found a record matching watever... so if the record also satisfies the CNF return the record
	//otherwise start scanning , if the next record doesnt satisfy query ordermaker return 0
	// else try it with the CNF, if yes return it, if no try the next record...n so on.
	getPage = page;
	pageToPick = middle+1;

	if(ce.Compare(record,&literal,&cnf)){
	  //printf("\n\nfound an exact match !!\n\n");
	  //record->Print(&mySchema);
	  fetchme.Consume(record);
	  return 1;
	}

	//printf("Checking other records...\n");
	//check other records
	while(GetNext(*record)){

	  ////printf("Checking this record :-\n");
	  //record->Print(&mySchema);

	  if(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)!=0){
	    //printf("next record did not match the query ordermaker\n");
	    //printf("did not match any record from end, soiry..\n");
	    restore_getPage(numRecsinGetpage,temp_page_to_pick);
	    return 0;
	  }
	  ////printf("next record did match the query ordermaker\n");
	  if(ce.Compare(record,&literal,&cnf)){
	    //printf("\n\nfound an exact match..\n\n");
	    //record->Print(&mySchema);
	    fetchme.Consume(record);
	    return 1;
	  }

	}

      }

      //did not match..oops...
      restore_getPage(numRecsinGetpage,temp_page_to_pick);
      //printf("did not match any record from end, soiry..\n");

      return 0;
    }
  }else if(getnext_called_in_succession==true){
    Record *record = new Record();
    if(query_ordermaker->numAtts >0){

      while(GetNext(*record)){

	////printf("Checking this record :-\n");
	//record->Print(&mySchema);

	if(ce.Compare(record,query_ordermaker,&literal,literal_queryOrderMaker)!=0){
	  //printf("\nnext record did not match the query ordermaker\n");
	  //printf("did not match any record from end, soiry..\n");
	  restore_getPage(numRecsinGetpage,temp_page_to_pick);
	  return 0;
	}
	//printf("next record did match the query ordermaker\n");
	if(ce.Compare(record,&literal,&cnf)){
	  //printf("\n\nfound an exact match..\n\n");
	  //record->Print(&mySchema);
	  fetchme.Consume(record);
	  return 1;
	}

      }
      restore_getPage(numRecsinGetpage,temp_page_to_pick);
      //printf("No more records...\n");
      return 0;
    }else{
      while(GetNext(*record)){
	if(ce.Compare(record,&literal,&cnf)){
	  fetchme.Consume(record);
	  return 1;
	}
      }
      restore_getPage(numRecsinGetpage,temp_page_to_pick);
      //printf("No more records...\n");
      return 0;

    }
  }
}



