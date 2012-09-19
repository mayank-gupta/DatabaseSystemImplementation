/*
 * TreeNode.h
 *
 *      Author: Mayank Gupta
 */

#ifndef TREENODE_H_
#define TREENODE_H_

#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "Function.h"
#include <string>
#include <iostream>
#include "Pipe.h"
#include "DBFile.h"
#include "RelOp.h"
#include "SelectFile.h"
#include "Schema.h"
#include "Project.h"
#include "Join.h"
#include "Sum.h"
#include "SelectPipe.h"
#include "DuplicateRemoval.h"

class TreeNode{
  public:
    TreeNode* leftChild;
    TreeNode* rightChild;
    Schema* schema;
    int inPipeID;
    int outPipeID;
    Pipe *inPipe;
    Pipe *outPipe;
    TreeNode(){
      leftChild = NULL;
      rightChild = NULL;
    }
    virtual ~TreeNode(){
    }
    virtual void ExecuteNode(){}
    virtual void PrintNode(){}
    virtual void WaitUntilDone(){}

};

class SelectFileNode : public TreeNode{

  string fileName;
  CNF *cnf;
  Record *literal;
  SelectFile *sf;
  DBFile dbfile;
  public:
  SelectFileNode(int outPipeID,string fileName,CNF* cnf, Record *literal, Pipe* outPipe, Schema* schema){

    this->outPipeID = outPipeID;
    this->fileName = fileName;
    this->cnf = cnf;
    this->literal = literal;
    this->outPipe = outPipe;
    this->sf = new SelectFile;
    this->schema = schema;
    leftChild=NULL;
    rightChild = NULL;
    sf->Use_n_Pages(100);
  }

  ~SelectFileNode(){
    if(cnf){
      delete cnf;
      cnf = NULL;
    }
    if(literal){
      delete literal;
      literal = NULL;
    }
    if(sf){
      delete sf;
      sf=NULL;
    }
  }

  void PrintNode(){
    if (this->leftChild != NULL)
      this->leftChild->PrintNode();

    cout << "\n******* Select File Operation ************";
    cout << "\nOutput pipe ID: " << this->outPipeID;
    cout << "\nInput filename: " << this->fileName.c_str();
    schema->PrintSchema();
    cout << "\nSelect CNF :\n";
    if (this->cnf != NULL)
      this->cnf->Print();
    else{
      cout << "NULL";
    }

    cout << endl << endl;
    if (this->rightChild != NULL)
      this->rightChild->PrintNode();

  }

  void ExecuteNode(){

    //create a DBFile from input file path provided

    dbfile.Open((char*)fileName.c_str());
    this->sf->Use_n_Pages(1000);

    sf->Run(dbfile, *outPipe, *cnf, *literal);



  }
  void WaitUntilDone(){
    sf->WaitUntilDone();
  }



};

class SelectPipeNode :public TreeNode{

  CNF *cnf;
  Record *literal;
  SelectPipe SP;
  public:
  SelectPipeNode(TreeNode* left,int inPipeID,Pipe* inPipe, int outPipeID,Pipe* outPipe, CNF* cnf, Record *literal,Schema* schema){
    this->leftChild = left;
    this->inPipeID = inPipeID;
    this->inPipe = inPipe;
    this->outPipeID = outPipeID;
    this->outPipe = outPipe;
    this->cnf = cnf;
    this->literal = literal;
    this->schema = schema;
  }

  ~SelectPipeNode(){
    if(cnf){
      delete cnf;
      cnf = NULL;
    }
    if(literal){
      delete literal;
      literal = NULL;
    }
  }

  void PrintNode(){
    if (this->leftChild != NULL)
      this->leftChild->PrintNode();

    cout<<"/n************ Select Pipe Operation ***********";
    cout<<"Input Pipe ID:"<<inPipeID<<endl;
    cout<<"Output Pipe ID: "<<outPipeID<<endl;
    schema->PrintSchema();
    cout << "\nSelect CNF :\n";
    if (this->cnf != NULL)
      this->cnf->Print();
    else{
      cout << "NULL";
    }

    cout<<endl;

  }

  void ExecuteNode(){
    if(leftChild!=NULL){
      leftChild->ExecuteNode();
    }

    SP.Run(*inPipe,*outPipe,*cnf,*literal);
  }



  void WaitUntilDone(){
    SP.WaitUntilDone();
  }
};

class JoinNode : public TreeNode{
  int inPipe1ID;
  int intPipe2ID;


  CNF *cnf;
  Record *literal;

  Pipe* inPipeLeft;
  Pipe* inPipeRight;
  Join J;
  public:

  JoinNode(TreeNode*left, TreeNode* right, int inPipe1ID,Pipe* inPipeLeft, int inPipe2ID,Pipe* inPipeRight, int outPipeID,Pipe* outPipe, CNF *cnf,Record* literal,Schema* schema){
    this->inPipe1ID = inPipe1ID;
    this->inPipeLeft = inPipeLeft;
    this->intPipe2ID = inPipe2ID;
    this->inPipeRight = inPipeRight;
    this->outPipeID = outPipeID;
    this->outPipe = outPipe;
    this->cnf = cnf;
    this->literal = literal;
    this->schema = schema;
    this->leftChild = left;
    this->rightChild = right;

  }

  ~JoinNode(){
    if(cnf){
      delete cnf;
      cnf = NULL;
    }
    if(literal){
      delete literal;
      literal = NULL;
    }
  }

  void PrintNode(){
    if(this->leftChild!=NULL){
      leftChild->PrintNode();
    }

    cout<<"\n***************** Join Operation ***************";
    cout<<"\nInput Pipe Left ID: "<<inPipe1ID;
    cout<<"\nInput Pipe Right ID: "<<intPipe2ID;
    cout<<"\nOutput Pipe ID: "<<outPipeID;
    schema->PrintSchema();
    cout << "\nJoin CNF :\n";
    if (this->cnf != NULL)
      this->cnf->Print();
    else{
      cout << "NULL means Block Nested Join";
    }

    if(this->rightChild!=NULL){
      rightChild->PrintNode();
    }
  }

  void ExecuteNode(){
    if(this->leftChild!=NULL)
      leftChild->ExecuteNode();

    if(this->rightChild!=NULL)
      rightChild->ExecuteNode();

    //leftChild->WaitUntilDone();
    //rightChild->WaitUntilDone();
    J.Run(*inPipeLeft,*inPipeRight,*outPipe,*cnf,*literal);

  }


  void WaitUntilDone(){
    leftChild->WaitUntilDone();
    rightChild->WaitUntilDone();
    J.WaitUntilDone();
  }

};

class ProjectNode: public TreeNode{

  int *keepme;
  int numsAttInput;
  int numsAttOutput;
  Project P;
  public:
  ProjectNode(TreeNode* left,int inPipeID,Pipe* inPipe, int outPipeID, Pipe* outPipe, int* keepme, int numAttsInput,int numAttsOutPut, Schema* schema){
    this->inPipeID = inPipeID;
    this->inPipe = inPipe;
    this->outPipeID = outPipeID;
    this->outPipe = outPipe;
    this->keepme = keepme;
    this->numsAttInput = numAttsInput;
    this->numsAttOutput = numAttsOutPut;
    this->schema = schema;
    this->leftChild = left;
    rightChild = NULL;
  }


  void PrintNode(){
    if(this->leftChild!=NULL){
      leftChild->PrintNode();
    }

    cout<<"\n************** Project Operation ******************\n";
    cout<<"Input Pipe ID:"<<inPipeID<<endl;
    cout<<"Output Pipe ID: "<<outPipeID<<endl;
    schema->PrintSchema();
    cout<<" Atts to keep: \n";
    for(int i=0;i<numsAttOutput;i++){
      cout<<keepme[i]<<" ";
    }
    cout<<endl;

  }

  void ExecuteNode(){
    if(this->leftChild!=NULL)
      leftChild->ExecuteNode();

    P.Run(*inPipe,*outPipe,keepme,numsAttInput,numsAttOutput);


  }

  void WaitUntilDone(){
    P.WaitUntilDone();
  }

};

class SumNode : public TreeNode{

  Function* function;
  Sum S;
  public:
  SumNode(TreeNode* left,int inPipeID, Pipe* inPipe, int outPipeID, Pipe* outPipe, Function* function, Schema* schema){
    this->leftChild = left;
    this->inPipeID = inPipeID;
    this->inPipe = inPipe;
    this->outPipeID = outPipeID;
    this->outPipe = outPipe;
    this->function = function;
    this->schema = schema;
  }


  void PrintNode(){
    if(this->leftChild!=NULL)
      leftChild->PrintNode();

    cout<<"\n*********** SUM Operation ****************";
    cout<<"\nInput Pipe ID:"<<inPipeID;
    cout<<"\nOutput Pipe ID: "<<outPipeID<<endl;
    schema->PrintSchema();
    cout<<"Function:\n";
    function->Print();

  }

  void ExecuteNode(){
    if(this->leftChild!=NULL)
      leftChild->ExecuteNode();
    S.Run(*inPipe,*outPipe,*function);
  }

  void WaitUntilDone(){
    S.WaitUntilDone();
  }



};



class GroupByNode : public TreeNode{

  OrderMaker *orderMaker;
  Function *function;
  GroupBy G;

  public:
  GroupByNode(TreeNode* left, int inPipeID, Pipe* inPipe, int outPipeID, Pipe* outPipe, OrderMaker* ordermaker, Function* function, Schema* schema){
    this->leftChild = left;
    this->inPipeID = inPipeID;
    this->inPipe = inPipe;
    this->outPipeID = outPipeID;
    this->outPipe = outPipe;
    this->orderMaker = ordermaker;
    this->function = function;
    this->schema = schema;
  }

  void PrintNode(){
    if (this->leftChild != NULL)
      this->leftChild->PrintNode();

    cout<<"/n********Group By Operation***********";
    cout<<"\nInput Pipe ID:"<<inPipeID;
    cout<<"\nOutput Pipe ID: "<<outPipeID<<endl;
    schema->PrintSchema();
    cout << "\nOrdermaker :\n";
    if (this->orderMaker != NULL)
      this->orderMaker->Print();
    else{
      cout << "NULL";
    }

    cout<<"\nFunction: \n";
    if(this->function!=NULL){
      this->function->Print();
    }else{
      cout<<" NULL\n";
    }

    cout<<endl;

  }

  void ExecuteNode(){
    if(this->leftChild!=NULL){
      leftChild->ExecuteNode();
    }

    G.Run(*inPipe,*outPipe,*orderMaker,*function);


  }


  void WaitUntilDone(){
    G.WaitUntilDone();
  }

};

class DuplicateRemovalNode: public TreeNode{
  DuplicateRemoval D;
  public:
  DuplicateRemovalNode(TreeNode* left, int inPipeID, Pipe* inPipe, int outPipeID, Pipe* outPipe,Schema* schema){
    this->leftChild = left;
    this->inPipeID = inPipeID;
    this->inPipe = inPipe;
    this->outPipeID = outPipeID;
    this->outPipe = outPipe;
    this->schema = schema;
  }

  void PrintNode(){
    if (this->leftChild != NULL)
      this->leftChild->PrintNode();

    cout<<"/n********* Duplicate Removal Operation ***********";
    cout<<"Input Pipe ID:"<<inPipeID<<endl;
    cout<<"Output Pipe ID: "<<outPipeID<<endl;
    schema->PrintSchema();

    cout<<endl;

  }

  void ExecuteNode(){
    if(this->leftChild!=NULL){
      leftChild->ExecuteNode();
    }

    D.Run(*inPipe,*outPipe,*schema);


  }

  void WaitUntilDone(){
    D.WaitUntilDone();
  }



};


#endif /* TREENODE_H_ */
