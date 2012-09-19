#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapDBFile.h"
#include "Defs.h"
#include <string.h>
#include <stdio.h>
#include <fstream>
extern struct AndList *final;

HeapDBFile::HeapDBFile() {
  file = new File();
  CurrentPage = new Page();
  getPage = new Page();
}

int HeapDBFile::Create(char *f_path, fType f_type, void *startup) {

  file->Open(0, f_path);
  std::ofstream out;
  char meta[100];
  sprintf(meta,"meta//%s.metadata",f_path);
  out.open(meta);
  int type=0;
  out<<type<<endl;
  out.close();
  file->Close();
  return 1;
}

int HeapDBFile::Open(char *f_path) {

  mode = reading;
  numRecsInOriginalGetPage = 0;

  struct stat buffer;
  if (stat(f_path, &buffer) == 0) {
    file->Open(1, f_path);
    pageToPick = 0;
    old_file_length = file->GetLength();
    return 1;
  } else
    return 0;

}

int HeapDBFile::Close() {

  if (mode == writing) {
    writing_to_reading();
  }
  file->Close();

  delete CurrentPage;
  delete getPage;
  return 1;

}

void HeapDBFile::Load(Schema &f_schema, char *loadpath) {

  if (mode == reading) {
    reading_to_writing();
  }
  FILE *tableFile = fopen(loadpath, "r");
  if (tableFile == NULL) {
    printf("Could not open the file to read \n");
  }
  Record *record = new Record();

  while (record->SuckNextRecord(&f_schema, tableFile)) {

    if (CurrentPage->Append(record) == 0) {

      if (file->GetLength() == 0)
	file->AddPage(CurrentPage, 0);
      else
	file->AddPage(CurrentPage, file->GetLength() - 1);

      CurrentPage->EmptyItOut();
      CurrentPage->Append(record);
    }
    record = new Record();
  }
}

void HeapDBFile::Add(Record &rec) {

  if (mode == reading) {
    reading_to_writing();
  }

  if (CurrentPage->Append(&rec) == 0) {
    if (file->GetLength() == 0) {
      file->AddPage(CurrentPage, 0);
    } else {
      file->AddPage(CurrentPage, file->GetLength() - 1);
    }
    CurrentPage->EmptyItOut();
    CurrentPage->Append(&rec);
  }
  //you just changed to writing, picked up page from disk, added two records, but page aint full and then
  // changed back to reading, so have to write this page back...at -2..not at -1..what to do?
}

int HeapDBFile::GetNext(Record &fetchme) {
  if(mode==writing){
    writing_to_reading();
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

int HeapDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
  if(mode==writing){
    writing_to_reading();
  }
  ComparisonEngine comp;
  int numRecsinGetpage = getPage->numRecs;
  int temp_page_to_pick = pageToPick;


  while (GetNext(fetchme)) {
    if (comp.Compare(&fetchme, &literal, &cnf)) {
      return 1;
    }
  }

  //failed so restore the original getpage and the page pointer
  pageToPick = temp_page_to_pick;
  //recreating the get page

  Record temp;
  file->GetPage(getPage,pageToPick-1);

  int till = getPage->numRecs - numRecsinGetpage;

  for(int i=0;i<(till);i++){

    getPage->GetFirst(&temp);
  }


  //printf("No more records past the current record match the CNF\n");
  return 0;

}

void HeapDBFile::MoveFirst() {
  pageToPick = 0;
  getPage->EmptyItOut();

}

void HeapDBFile::reading_to_writing() {
  mode = writing;
  if (file->GetLength() > 0) {
    //printf("got page %d\n",file->GetLength()-2);
    file->GetPage(CurrentPage, file->GetLength() - 2);
  }

}

void HeapDBFile::writing_to_reading() {
  mode = reading;

  if (CurrentPage->numRecs > 0) {
    if (file->GetLength() == 0) {
      printf("Added first page..\n");
      file->AddPage(CurrentPage, 0);
      old_file_length = file->GetLength();
    } else {
      if(file->GetLength()>old_file_length){
	file->AddPage(CurrentPage, file->GetLength() - 1);
	old_file_length = file->GetLength();
      }
      else
	file->AddPage(CurrentPage, file->GetLength() - 2);

    }
    CurrentPage->EmptyItOut();
  }


  if(pageToPick>0){
    Record record;
    //now update the getpage from disk

    int num_recs_in_current_get_page = getPage->numRecs;
    //printf("pages in original = %d\n",numRecsInOriginalGetPage);
    //printf("pages in current = %d\n",num_recs_in_current_get_page);
    file->GetPage(getPage,pageToPick-1);
    int num_recs_in_new_get_page = getPage->numRecs;

    printf("pages in new = %d\n",num_recs_in_new_get_page);
    if(num_recs_in_new_get_page>numRecsInOriginalGetPage){
      for(int i=0;i<(numRecsInOriginalGetPage-num_recs_in_current_get_page);i++){
	//printf("removing stale records..\n");
	getPage->GetFirst(&record);
      }
      numRecsInOriginalGetPage = num_recs_in_new_get_page;
    }

  }
}
