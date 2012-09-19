#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include  "HeapDBFile.h"
#include  "SortedDBFile.h"
#include "GenericDBFile.h"
#include<fstream>

// stub file .. replace it with your own DBFile.cc



DBFile::DBFile () {

}


int DBFile::Create (char *f_path, fType f_type, void *startup) {

  if(f_type == 0){
    genericDBFile = new HeapDBFile();
    genericDBFile->Create(f_path,f_type,startup);
    return 1;
  }else if(f_type== 1){
    genericDBFile = new SortedDBFile();
    genericDBFile->Create(f_path,f_type,startup);
    return 1;
  }else{
    printf("Did not recognize the file type..exiting\n");
    return 0;
  }
  return 0;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
  genericDBFile->Load(f_schema,loadpath);
}

int DBFile::Open (char *f_path) {

  char meta[100];
  sprintf(meta,"meta//%s.metadata",f_path);
  std::ifstream in;
  in.open(meta);
  int type;
  if(in.is_open()){
    in>>type;
  }
  in.close();

  fType f_type = (fType) type;

  if(f_type == 0){
    printf("Opened a Heap file\n");
    genericDBFile = new HeapDBFile();
    genericDBFile->Open(f_path);
    return 1;
  }else if(f_type== 1){
    printf("Opened a sorted file\n");
    genericDBFile = new SortedDBFile();
    genericDBFile->Open(f_path);
    return 1;
  }else{
    printf("Did not recognize the file type..exiting\n");
    return 0;
  }


  return 0;
}



int DBFile::Close () {

  return genericDBFile->Close();


}
void DBFile::Add (Record &rec) {
  genericDBFile->Add(rec);
}

void DBFile::MoveFirst() {
  genericDBFile->MoveFirst();

}

int DBFile::GetNext(Record &fetchme) {
  return genericDBFile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  return genericDBFile->GetNext(fetchme,cnf,literal);
}



