/*
 * DBS.cc
 *
 *      Author: Mayank Gupta
 */

#include "DBS.h"
#include <vector>
#include <algorithm>
#include <string.h>
#include <stdio.h>

void printAttrAndTypeList(struct AttrAndTypeList *ptr){

  while(ptr !=NULL){
    cout<<"ATTRIBUTE name: "<<ptr->name<<"  ATTRIB Type: "<<ptr->type<<endl<<endl;
    ptr=ptr->next;
  }

}


int Database::checkTablePresence(char *tablename){
  //myTable lookup in CATALOG
  FILE *foo = fopen (catalog_path, "r");

  if(foo==NULL){
    return 1;
  }
  char space[200];
  fscanf (foo, "%s", space);
  /*
     if (strcmp (space, "BEGIN")) {
     cout << "Unfortunately, this does not seem to be a schema file.\n";
     exit (1);
     }

*/
  while (1) {
    // check to see if tablename is present
    fscanf (foo, "%s", space);
    if (strcmp (space, tablename)) {
      // it is not, so suck up everything to past the BEGIN
      while (1) {
	// suck up another token
	if (fscanf (foo, "%s", space) == EOF) {
	  return 1;
	}
	if (!strcmp (space, "BEGIN")) {
	  break;
	}
      }
      // otherwise, got the correct file!!
    } else {
      break;
    }
  }
  return 0;
}

void Database::appendCatalog(AttrAndTypeList *revList){

  //apply test condition for File open success
  FILE * catfile = fopen (catalog_path, "a");

  //linked list reversal...
  AttrAndTypeList *head=NULL;
  AttrAndTypeList *temp=NULL;
  //it will have atleast 1 element node
  temp= revList;
  revList=revList->next;
  temp->next=NULL;
  head=temp;
  while(revList !=NULL)
  {
    temp= revList;
    revList=revList->next;
    temp->next=head;
    head=temp;
  }
  //now appending table info to file
  fprintf (catfile, "\nBEGIN\n%s\n%s.tbl", myTable, myTable);
  while(head !=NULL){
    fprintf (catfile, "\n%s ",head->name);
    if (strcmp(head->type,"INTEGER")==0)
      fprintf (catfile, "Int");
    if (strcmp(head->type,"DOUBLE")==0)
      fprintf (catfile, "Double");
    if (strcmp(head->type,"STRING")==0)
      fprintf (catfile, "String");
    head=head->next;
  }

  fprintf (catfile, "\nEND\n");
  fclose(catfile);

}

int Database::cleanCatalog(){


  ifstream infile(catalog_path);
  string line;
  ofstream outfile("tempCatalog.txt");

  if(infile.is_open()){

    while(infile.good()){

      getline(infile,line);
      if(line.compare("BEGIN")==0){
	string tableName;
	getline(infile,tableName);
	if(strcmp((char*)tableName.c_str(),myTable)!=0){
	  outfile<<"BEGIN\n";
	  outfile<<tableName<<endl;
	  string checkEnd;
	  getline(infile,checkEnd);
	  while(checkEnd.compare("END")!=0){
	    outfile<<checkEnd<<endl;
	    getline(infile,checkEnd);
	  }
	  outfile<<"END"<<endl<<endl;
	}else{
	  while(tableName.compare("END")!=0){
	    getline(infile,tableName);
	  }

	}
      }
      //cout<<line<<endl;
    }

    remove(catalog_path);
    rename("tempCatalog.txt",catalog_path);
    return 1;
  }else{
    cout<<"\nERROR ! Could not open catalog for reading\n";

  }

  return 0;

}
void Database::CreateTable(char *fpath, fType file_type, void *startup){

  DBFile dbfile;
  cout << "\n Creating bin file at  : " <<fpath<< endl;
  dbfile.Create(fpath,file_type,&startup);
  dbfile.Close ();
  cout<<" The Table has been Created "<<endl;
  /*cout<<" The attribute and types , list for the table to be created is \n ";
    AttrAndTypeList *pairPtr ;
    pairPtr= pairList;
    printAttrAndTypeList(pairPtr);
    cout<<endl;*/


}

void Database::InsertInto(){

  if(checkTablePresence(myTable)){
    cout<<"\n ERROR ! Table not present in the schema, first do a CREATE TABLE\n";
    return;
  }
  DBFile dbfile;
  char tbl_path[300]; // construct path of the tpch (flat text file)
  char path[300];
  sprintf (path, "%s%s.bin", dbfile_dir, myTable);
  sprintf (tbl_path, "%s%s", tpch_dir,loadFile);
  cout << " tpch file will be loaded from " << tbl_path << endl;

  Schema s= Schema (catalog_path, myTable);
  dbfile.Open(path);
  dbfile.Load (s, tbl_path);
  dbfile.Close ();

}

int Database::DropTable(){
  //we have to delete the information of myTable from catalog and remove file myTable.bin

  //removing schema information from catalog

  if(!checkTablePresence(myTable)){

    cleanCatalog();

    //removing bin file
    char path[300];
    sprintf (path, "%s%s.bin", dbfile_dir, myTable);
    if( remove(path) != 0 )
      printf( "\nError deleting bin file" );
    else
      printf( "\nbin File successfully deleted" );

    //removing meta file
    char metapath[300];
    sprintf (metapath, "%smeta/%s.bin.metadata", dbfile_dir, myTable);

    if( remove(metapath) != 0 )
      printf( "\nError deleting metadata file" );
    else
      printf( "\nbin Metadata File successfully deleted" );



    return 0;
  }else{
    cout<<"\nTable does not exist, nothing to drop\n";
    return 1;
  }

}

void Database::SetOutput(){
  //just set a flag according to input case...default taken as STDOUT
  ifstream infile;
  infile.open("OUTPUTFLAG");
  string line;
  if (infile.is_open()){
    while (infile.good())
    {
      getline (infile,line);
      cout <<"Changing output location from " <<line<< "to  -: "<<outputDest<<endl;
    }
    infile.close();
  }
  else cout << "Unable to open file";
  ofstream outfile;
  outfile.open("OUTPUTFLAG");
  outfile<<outputDest;



}

void Database::ExecuteSelectQuery(){
  // according to output flag execute the query

  queryOptimizer.getAndOptimizeQuery();
  ifstream infile("OUTPUTFLAG");
  string line;
  getline(infile,line);

  if(line.compare("STDOUT")==0){
    queryOptimizer.ExecuteTree(1);
  }else if(line.compare("NONE")==0){
    queryOptimizer.PrintQueryPlan();
    //qury plan
  }else{
    //to file filename

    queryOptimizer.ExecuteTree(2);
  }


}
void Database::Execute(){
  if (queryId == 1){
    char path[300];
    //check presence of table
    if(checkTablePresence(myTable)== 0){
      cout<< "table already present";
      exit(1);
    }
    //append CATALOG
    AttrAndTypeList *appendCat=NULL;
    appendCat= pairList;
    appendCatalog(appendCat); //here improvement in code required, also taking catalog from global
    // Create a valid file path name where to create .bin file
    sprintf (path, "%s%s.bin", dbfile_dir, myTable);
    //create ordermaker for sorted files
    /*OrderMaker o;
      int runlen = 10;
      struct {
      OrderMaker *o;
      int l;
      } startup = {&o, runlen};*/


    if(strcmp(fileType, "HEAP")== 0){
      //call CreateTable function for HEAP with Schema
      CreateTable(path, heap,NULL);
    }
    else if(strcmp(fileType,"SORTED")== 0) {
      //call CreateTable with Schema and Attributes to Sort over.

      cout<<"To BE handled "<<endl;
      /*
	 NameList *nameListptr;
	 nameListptr = groupingAtts;
	 PrintNameList(nameListptr);*/
    }
    else if (strcmp(fileType,"BPLUS")== 0){
      cout<<" TO BE SUPPORTED"<<endl;
    }
  }
  else if(queryId ==2){
    cout<<" NOW BULK LOADING the :"<<loadFile<<"  into Relation :"<<myTable<<endl;
    InsertInto();

  }
  else if(queryId== 3){
    cout<<" Now Deleting Table :"<<myTable<<endl;
    DropTable();

  }
  else if(queryId == 4){
    cout<<"Now Setting the Output mode to :"<<outputDest<<endl;
    SetOutput();
  }
  else if (queryId == 5){
    cout<<"Now Select Query being called :"<<endl;
    ExecuteSelectQuery();

  }
  else if (queryId == 6) {
    cout<< "STAT UPDATE yet to be implemented "<<endl;

  }/*
      else
      cout<< " QUERY DID NOT MATCH TO SUPPORTED TYPE \n";
      */


}

int main () {
  cout<<"------------STARTING DATABASE SYSTEM----------\n";
  cout<<"===============================================\n";
  cout<<"\nPlease Enter a SQL query and press ctrl-D\n\n\n\n";

  if (yyparse() != 0) {
    cout << " Error: can't parse your CNF \n";

  }
  Database dbi;
  dbi.Execute();
  return 0;

}
