/*
 * QueryOptimizer.h
 *
 *      Author: Mayank Gupta
 */

#ifndef QUERYOPTIMIZER_H_
#define QUERYOPTIMIZER_H_

// the aggregate function (NULL if no agg)

#include <iostream>
#include <stdio.h>
#include <map>
#include "ParseTree.h"
#include "Statistics.h"
#include "Schema.h"
#include "TreeNode.h"
#include "Comparison.h"
#include "Pipe.h"
#include "WriteOut.h"
#include <fstream>

extern struct AndList *boolean;
extern struct TableList *tables;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;
extern struct FuncOperator *finalFunction;



extern "C" {
  int yyparse(void);   // defined in y.tab.c
}


using namespace std;


typedef map<string,AndList*> SelectMap;
typedef map<string,AndList*> JoinMap;
typedef map<string,string> TableMap;
typedef map<string,double> JoinCostMap;
typedef map<string,Schema*> SchemaMap;
class QueryOptimizer{
  SelectMap selmap;
  SelectMap joinSelectMap;
  JoinMap joinMap;
  TableMap tableToAliasMap;
  TableMap aliasToTableMap;
  Statistics statistics;
  JoinCostMap costMap;
  string optimalJoinOrder;
  int nextPipeID;
  SchemaMap aliasToSchemaMap;
  TreeNode *root;
  string joinedNameSoFar;
  public:

  QueryOptimizer(){
    nextPipeID = 0;
    joinedNameSoFar="";
  }
  void getAndOptimizeQuery();
  void scanSelections();
  void generateStatisticsFile();
  void parseTableNames();
  void populateSchemaMap(string,string);
  void populateSelectionHash(string attValue, OrList* orlist,bool& isNewList);
  void populateJoinSelectionHash(AndList*);
  string getTableName(string);
  void populateJoinMap(OrList* orlist);
  void scanJoins();
  void EnumerateAllPossibleJoins();
  string computeOptimalJoins(vector<string> aliases, Statistics &s);
  AndList* getAndList(vector<string> leftTableAliasVector,string leftTableName);
  AndList* findJoinSelect(string name);
  void BuildTree();
  void PrintQueryPlan();
  TreeNode* createSelectNode(string aliasName);
  TreeNode* createProjectNode(Schema*);
  TreeNode* createJoinNode(TreeNode* left, TreeNode* right,string rightTableName);
  void ExecuteTree(int);
  AndList* getAndListForRealJoin(vector<string>,string);
  TreeNode* createSumNode();
  TreeNode* createSelectPipeNode(AndList*);
  TreeNode* createGroupByNode();
  void getOrderMaker(OrderMaker* ordermaker, Schema* schema);
  TreeNode* createDuplicateRemovalNode();
};



#endif /* QUERYOPTIMIZER_H_ */
