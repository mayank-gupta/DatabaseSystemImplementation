#ifndef SortedDBFile_H
#define SortedDBFile_H


#include "GenericDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Defs.h"


class SortedDBFile: public GenericDBFile  {
  friend class OrderMaker;
  friend class Page;
  friend class File;
  friend class Record;
  private:
  OrderMaker* orderMaker;

  bool getnext_called_in_succession;
  fType type;
  char* f_path;
  int runLength;
  File* file;
  Mode mode;
  BigQ* bigq;
  Pipe* input;
  Pipe* output;
  ComparisonEngine ce;
  int pageToPick;
  Page* getPage;
  int numRecsInOriginalGetPage;
  OrderMaker* literal_queryOrderMaker;
  OrderMaker* query_ordermaker;
  public:

  SortedDBFile ();

  int Create (char *fpath, fType file_type, void *startup);
  int Open (char *fpath);
  int Close ();

  void Load (Schema &myschema, char *loadpath);

  void MoveFirst ();
  void Add (Record &addme);
  int GetNext (Record &fetchme);
  int GetNext (Record &fetchme, CNF &cnf, Record &literal);
  void readingToWriting();
  void writingToReading();
  void write_orderMaker_to_disk();
  void create_orderMaker_from_disk();
  void print_file(char*);
  void restore_getPage(int,int);
};
#endif
