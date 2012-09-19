/*
 * HeapDBFile.h
 *
 *      Author: Mayank Gupta
 */

#ifndef HEAPDBFILE_H_
#define HEAPDBFILE_H_

#include "GenericDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Defs.h"


class HeapDBFile: public GenericDBFile {

  friend class Page;
  friend class File;
  private:
  File *file;
  Page *getPage;
  Page *CurrentPage;
  int numRecsInOriginalGetPage;
  int pageToPick;
  Mode mode;
  int old_file_length;
  public:
  HeapDBFile();


  int Create (char *fpath, fType file_type, void *startup);
  int Open (char *fpath);
  int Close ();

  void Load (Schema &myschema, char *loadpath);

  void MoveFirst ();
  void Add (Record &addme);
  int GetNext (Record &fetchme);
  int GetNext (Record &fetchme, CNF &cnf, Record &literal);
  void reading_to_writing();
  void writing_to_reading();

};


#endif /* HEAPDBFILE_H_ */
