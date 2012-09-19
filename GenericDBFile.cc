/*
 * GenericDBFile.cpp
 *
 *      Author: Mayank Gupta
 */

#include "GenericDBFile.h"

GenericDBFile::GenericDBFile() {
  // TODO Auto-generated constructor stub

}

GenericDBFile::~GenericDBFile() {

}

int GenericDBFile :: Create (char *fpath, fType file_type, void *startup){return 0;}
int GenericDBFile ::Open (char *fpath){return 0;}
int GenericDBFile ::Close (){return 0;}

void GenericDBFile ::Load (Schema &myschema, char *loadpath){}

void GenericDBFile ::MoveFirst (){}
void GenericDBFile ::Add (Record &addme){}
int GenericDBFile :: GetNext (Record &fetchme){return 0;}
int GenericDBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal){return 0;}
