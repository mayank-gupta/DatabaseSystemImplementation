#ifndef DBS_H
#define	DBS_H

#include<iostream>
#include<string>
#include<fstream>
#include"File.h"
#include"BigQ.h"
#include"HeapDBFile.h"
#include"SortedDBFile.h"
#include"Statistics.h"
#include"Schema.h"
#include"Defs.h"
#include"GenericDBFile.h"
#include"Comparison.h"
#include"ComparisonEngine.h"
#include"DBFile.h"
#include"DuplicateRemoval.h"
#include"QueryOptimizer.h"

using namespace std;

class Database{
  private:
    int queryId;
    char* myTable;
    char* dbfile_dir;
    AttrAndTypeList *pairList;
    char *fileType;
    char *outputDest;
    char *catalog_path;
    char *loadFile;
    char *tpch_dir;
    QueryOptimizer queryOptimizer;
  public: 
    int checkTablePresence(char* tablename);
    void appendCatalog(AttrAndTypeList *revList);
    int cleanCatalog();
    void CreateTable(char *fpath, fType file_type, void *startup);
    void InsertInto();
    int DropTable();
    void SetOutput();
    void ExecuteSelectQuery();
    void Execute();
};
#endif
