/*
 * GenericDBFile.h
 *
 *      Author: Mayank Gupta
 */

#ifndef GENERICDBFILE_H_
#define GENERICDBFILE_H_



#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"


class GenericDBFile {
  public:
    GenericDBFile();
    virtual int Create (char *fpath, fType file_type, void *startup);
    virtual int Open (char *fpath);
    virtual int Close ();

    virtual void Load (Schema &myschema, char *loadpath);

    virtual void MoveFirst ();
    virtual void Add (Record &addme);
    virtual int GetNext (Record &fetchme);
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    virtual ~GenericDBFile();
};

#endif /* GENERICDBFILE_H_ */
