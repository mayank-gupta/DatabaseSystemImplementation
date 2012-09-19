/*
 * QueryOptimizer.cc
 *
 *      Author: Mayank Gupta
 */

#include "QueryOptimizer.h"
#include <sstream>
#include <set>
#include <string.h>
#include <algorithm>

int pipe_size = 10000000;


void QueryOptimizer::getAndOptimizeQuery(){
  //cout<<"Enter the query\n";
  //yyparse();
  generateStatisticsFile();
  parseTableNames();
  scanSelections();
  scanJoins();
  EnumerateAllPossibleJoins();
  BuildTree();
  //PrintQueryPlan();
  //ExecuteTree();
  /*
     PrintAndList(boolean);
     cout<<endl;
     PrintTableList(tables);
     PrintNameList(groupingAtts);
     PrintNameList(attsToSelect);
     cout<<"DistinctAtts "<<distinctAtts<<endl;
     cout<<"Distinct Func "<<distinctFunc<<endl;

     cout<<"Printing the function operand thingy\n";
     PrintFuncOperator(finalFunction,2);

*/




}

void PrintOperand(struct Operand *pOperand)
{
  if(pOperand!=NULL)
  {
    cout<<pOperand->value<<" ";
  }
  else
    return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
  if(pCom!=NULL)
  {
    PrintOperand(pCom->left);
    switch(pCom->code)
    {
      case 1:
	cout<<" < "; break;
      case 2:
	cout<<" > "; break;
      case 3:
	cout<<" = ";
    }
    PrintOperand(pCom->right);

  }
  else
  {
    return;
  }
}
void PrintOrList(struct OrList *pOr)
{
  if(pOr !=NULL)
  {
    struct ComparisonOp *pCom = pOr->left;
    PrintComparisonOp(pCom);

    if(pOr->rightOr)
    {
      cout<<" OR ";
      PrintOrList(pOr->rightOr);
    }
  }
  else
  {
    return;
  }
}
void PrintFuncOperator(struct FuncOperator *funclist, int intend)
{
  if(funclist)
  {
    for(int i=1; i<= intend; i++)
      cout << "  ";
    cout << (char)funclist -> code  << " ";
    if(funclist -> leftOperand)
    {
      cout	<< "(";
      switch(funclist -> leftOperand -> code)
      {
	case STRING:
	  cout << "STRING";
	  break;
	case DOUBLE:
	  cout << "DOUBLE";
	  break;
	case INT:
	  cout << "INT";
	  break;
	case NAME:
	  cout << "NAME";
	  break;
      }
      cout	<< "," << funclist -> leftOperand -> value << ")";
    }
    cout	<< "\n";
    PrintFuncOperator(funclist -> leftOperator, intend + 1);
    PrintFuncOperator(funclist -> right, intend + 1);
  }
}


void PrintAndList(struct AndList *pAnd)
{
  if(pAnd !=NULL)
  {
    struct OrList *pOr = pAnd->left;
    PrintOrList(pOr);
    if(pAnd->rightAnd)
    {
      cout<<" AND ";
      PrintAndList(pAnd->rightAnd);
    }
  }
  else
  {
    return;
  }
}

void PrintTableList(struct TableList *tables){

  cout<<"Table Name: "<<tables->tableName<<" Alias: "<<tables->aliasAs<<endl;
  if(tables->next)
    PrintTableList(tables->next);
}

void PrintNameList(struct NameList *groupatts){
  cout<<groupatts->name<<endl;
  if(groupatts->next)
    PrintNameList(groupatts->next);

}




//delete the new schemas formed if time permits
void QueryOptimizer::populateSchemaMap(string alias,string tableName){

  Schema schema("catalog",(char*)tableName.c_str());

  Attribute* atts = schema.GetAtts();

  Attribute *aliasAtts = new Attribute[schema.numAtts];
  string attName;
  for(int i=0;i<schema.numAtts;i++){
    attName ="";
    attName.append(alias);
    attName.append(".");
    attName.append(atts[i].name);
    char* aliasAttname = new char[50];
    strcpy(aliasAttname,(char*) attName.c_str());
    aliasAtts[i].name = aliasAttname;
    aliasAtts[i].myType = atts[i].myType;
  }

  Schema *aliasSchema = new Schema((char*)alias.c_str(),schema.numAtts,aliasAtts);

  aliasToSchemaMap.insert(pair<string,Schema*>(alias,aliasSchema));
}

//convert tables names into map<alias,tablename>
void QueryOptimizer::parseTableNames(){
  TableList* tableListPtr = tables;
  while(tableListPtr!=NULL){
    string alias,tableName;
    alias = tableListPtr->aliasAs;
    tableName =  tableListPtr->tableName;
    tableToAliasMap.insert(pair<string,string>(tableName,alias));
    aliasToTableMap.insert(pair<string,string>(alias,tableName));
    statistics.CopyRel((char*)tableName.c_str(),(char*)alias.c_str());
    tableListPtr = tableListPtr->next;
    populateSchemaMap(alias,tableName);
  }

  statistics.Write("Statistics.txt");
  /*
     TableMap::iterator it;
     for(it = tableNames.begin();it!=tableNames.end();it++){
     cout<<it->first<<" "<<it->second<<endl;
     }
     */
}

void QueryOptimizer::generateStatisticsFile(){

  statistics.AddRel("region",5);
  statistics.AddRel("nation",25);
  statistics.AddRel("part",200000);
  statistics.AddRel("supplier",10000);
  statistics.AddRel("partsupp",800000);
  statistics.AddRel("customer",150000);
  statistics.AddRel("orders",1500000);
  statistics.AddRel("lineitem",6001215);

  // add attributes

  statistics.AddAtt("region","r_regionkey",5);
  statistics.AddAtt("region","r_name",5);
  statistics.AddAtt("region","r_comment",5);

  statistics.AddAtt("nation","n_nationkey",25);
  statistics.AddAtt("nation","n_name",25);
  statistics.AddAtt("nation","n_regionkey",5);
  statistics.AddAtt("nation","n_comment",25);

  statistics.AddAtt("part","p_partkey",200000);
  statistics.AddAtt("part","p_name",199996);
  statistics.AddAtt("part","p_mfgr",5);
  statistics.AddAtt("part","p_brand",25);
  statistics.AddAtt("part","p_type",150);
  statistics.AddAtt("part","p_size",50);
  statistics.AddAtt("part","p_container",40);
  statistics.AddAtt("part","p_retailprice",20899);
  statistics.AddAtt("part","p_comment",127459);

  statistics.AddAtt("supplier","s_suppkey",10000);
  statistics.AddAtt("supplier","s_name",10000);
  statistics.AddAtt("supplier","s_address",10000);
  statistics.AddAtt("supplier","s_nationkey",25);
  statistics.AddAtt("supplier","s_phone",10000);
  statistics.AddAtt("supplier","s_acctbal",9955);
  statistics.AddAtt("supplier","s_comment",10000);

  statistics.AddAtt("partsupp","ps_partkey",200000);
  statistics.AddAtt("partsupp","ps_suppkey",10000);
  statistics.AddAtt("partsupp","ps_availqty",9999);
  statistics.AddAtt("partsupp","ps_supplycost",99865);
  statistics.AddAtt("partsupp","ps_comment",799123);

  statistics.AddAtt("customer","c_custkey",150000);
  statistics.AddAtt("customer","c_name",150000);
  statistics.AddAtt("customer","c_address",150000);
  statistics.AddAtt("customer","c_nationkey",25);
  statistics.AddAtt("customer","c_phone",150000);
  statistics.AddAtt("customer","c_acctbal",140187);
  statistics.AddAtt("customer","c_mktsegment",5);
  statistics.AddAtt("customer","c_comment",149965);

  statistics.AddAtt("orders","o_orderkey",1500000);
  statistics.AddAtt("orders","o_custkey",99996);
  statistics.AddAtt("orders","o_orderstatus",3);
  statistics.AddAtt("orders","o_totalprice",1464556);
  statistics.AddAtt("orders","o_orderdate",2406);
  statistics.AddAtt("orders","o_orderpriority",5);
  statistics.AddAtt("orders","o_clerk",1000);
  statistics.AddAtt("orders","o_shippriority",1);
  statistics.AddAtt("orders","o_comment",1478684);

  statistics.AddAtt("lineitem","l_orderkey",1500000);
  statistics.AddAtt("lineitem","l_partkey",200000);
  statistics.AddAtt("lineitem","l_suppkey",10000);
  statistics.AddAtt("lineitem","l_linenumber",7);
  statistics.AddAtt("lineitem","l_quantity",50);
  statistics.AddAtt("lineitem","l_extendedprice",933900);
  statistics.AddAtt("lineitem","l_discount",11);
  statistics.AddAtt("lineitem","l_tax",9);
  statistics.AddAtt("lineitem","l_returnflag",3);
  statistics.AddAtt("lineitem","l_linestatus",2);
  statistics.AddAtt("lineitem","l_shipdate",2526);
  statistics.AddAtt("lineitem","l_commitdate",2466);
  statistics.AddAtt("lineitem","l_receiptdate",2554);
  statistics.AddAtt("lineitem","l_shipinstruct",4);
  statistics.AddAtt("lineitem","l_shipmode",7);
  statistics.AddAtt("lineitem","l_comment",4501941);

  statistics.Write("Statistics.txt");

}

void QueryOptimizer::populateSelectionHash(string attValue, OrList* orlist, bool &isNewList){
  size_t dotPos = attValue.find(".");
  string aliasName = attValue.substr(0,dotPos);
  //cout<<aliasName<<endl;
  string attName = attValue.substr(dotPos+1);
  //cout<<attName<<endl;


  AndList *newAndList;
  SelectMap::iterator it;
  it = selmap.find(aliasName);
  if(it==selmap.end()){
    newAndList = new AndList();
    newAndList->rightAnd = NULL;
    newAndList->left = orlist;
    selmap.insert(pair<string,AndList*>(aliasName,newAndList));
  }
  else{
    //go to the end of the ANDLIST
    AndList *tempAndList = it->second;
    while(tempAndList->rightAnd!=NULL){
      tempAndList = tempAndList->rightAnd;
    }

    if(isNewList==true){
      //if its a new AND, make a new ANDLIST and add it to the list
      newAndList = new AndList();
      newAndList->rightAnd = NULL;
      newAndList->left = orlist;
      tempAndList->rightAnd = newAndList;
      isNewList = false;
    }else{
      //if not, then append it to the orlist of the last Andlist
      OrList* tempOrList = tempAndList->left;
      while(tempOrList->rightOr!=NULL){
	tempOrList = tempOrList->rightOr;
      }
      tempOrList->rightOr = orlist;
    }
  }

  /*
     for(it = selmap.begin();it!=selmap.end();it++){
     cout<<"Alias: "<<it->first<<"  AndList: ";
     PrintAndList(it->second);
     cout<<endl;
     }

     cout<<endl<<endl;
     */
}

string QueryOptimizer::getTableName(string attName){
  // iterate the table names map
  cout<<endl;
  TableMap::iterator it;
  for(it = tableToAliasMap.begin();it!=tableToAliasMap.end();it++){
    //get the schema
    string tableName = it->first;
    Schema schema("catalog",(char*)tableName.c_str());
    //cout<<"TableName: "<<tableName<<"  Atts: "<<schema.GetNumAtts()<<endl;

    //find if the attribute is in the schema
    if(schema.Find((char*)attName.c_str()) != -1){
      //cout<<"Success Table Name: "<<tableName<<endl;
      return tableName;
    }
  }

  cout<<"Error ! Could not find the attribute in any of the tables in catalog";
  return "-1";
}

//join select shoudl have only one Orlist in Orlist*
void QueryOptimizer::populateJoinSelectionHash(AndList* andList){
  PrintAndList(andList);

  //vector<string> attNames;
  set<string> tableNamesSet;
  OrList* orlist = andList->left;

  while(orlist!=NULL){
    string attName = orlist->left->left->value;
    string tableName = getTableName(attName);
    string alias = tableToAliasMap[tableName];
    tableNamesSet.insert(alias);

    //concatenate the aliasnames to attribute names

    char *newAttName = new char[50];
    string updatedAttName = alias + "." + attName;
    //check if this brings out a memory corruption sort of thing
    strcpy(newAttName,(char*) updatedAttName.c_str());
    orlist->left->left->value = newAttName;
    orlist = orlist->rightOr;


  }

  //make the final joined table name like l.o
  set<string>::iterator it;
  string joinedTableName="";
  for(it = tableNamesSet.begin();it!=tableNamesSet.end();it++){
    joinedTableName  = joinedTableName + *it + ".";
    cout<<*it<<endl;
  }
  joinedTableName = joinedTableName.substr(0,joinedTableName.size()-1);
  //jointablename now stores the final tablename which needs to be inputed into the hash
  cout<<joinedTableName<<endl;

  joinSelectMap.insert(pair<string,AndList*>(joinedTableName,andList));

  /*
     SelectMap::iterator itr;
  //print the map to see if everything is working fine

  for(itr = joinSelectMap.begin();itr!=joinSelectMap.end();itr++){
  cout<<"Table Name: "<<itr->first<<"  AndList: ";
  PrintAndList(itr->second);
  cout<<endl;
  }

*/

}

void QueryOptimizer::scanSelections(){
  AndList* andListPtr = boolean;
  OrList* orListptr;
  OrList* prev;
  OrList* next;
  ComparisonOp *compOpPtr;
  AndList *prevAndList = NULL;

  while(andListPtr!= NULL){
    bool isNewList = true; // tells if its a new andlist or not
    orListptr = andListPtr->left;
    while(orListptr!=NULL){
      next = orListptr->rightOr;
      compOpPtr = orListptr->left;
      if(compOpPtr ->right->code!= NAME){
	string attValue = compOpPtr->left->value;
	//break the orlist and pass it to appropriate function


	if(attValue.find(".")== -1){
	  //special join select case
	  //take out the ANDLIST chunk
	  if(prevAndList!=NULL){
	    prevAndList->rightAnd = andListPtr->rightAnd;
	    andListPtr->rightAnd = NULL;
	    //call your function with AndList
	    populateJoinSelectionHash(andListPtr);
	    andListPtr = prevAndList;
	  }else{
	    AndList *temp = andListPtr->rightAnd;
	    andListPtr->rightAnd=NULL;
	    populateJoinSelectionHash(andListPtr);
	    andListPtr = temp;
	  }
	  break;
	}
	else{
	  //simple case of select
	  // take out a Orlist chunk and pass it to function
	  prev = orListptr;
	  prev->rightOr = NULL;
	  populateSelectionHash(attValue,prev,isNewList);
	}

      }

      orListptr = next;
    }

    //to deal the case when the join select occurs in the first andlist itself;
    if(prev!=NULL)
      prevAndList = andListPtr;

    andListPtr = andListPtr->rightAnd;
  }
}

//does not handle the case where two relations are joined over multiple attributes
void QueryOptimizer::populateJoinMap(OrList* orlist){
  AndList* andlist = new AndList;
  andlist->left = orlist;
  andlist->rightAnd = NULL;
  //cout<<"Printing the info from populate Join \n";
  string attName1,attName2;
  attName1 = orlist->left->left->value;
  attName2 = orlist->left->right->value;

  //cout<<attName1<<" "<<attName2<<endl;

  size_t dotPos;
  dotPos = attName1.find(".");
  string aliasName1 = attName1.substr(0,dotPos);
  //cout<<aliasName1<<endl;

  dotPos = attName2.find(".");
  string aliasName2 = attName2.substr(0,dotPos);
  //cout<<aliasName2<<endl;

  //sort and insert the joined name into the join map
  string joinedName="";
  if(aliasName1.compare(aliasName2)<=0){
    joinedName = aliasName1 + "." + aliasName2;
  }else
    joinedName = aliasName2 + "." + aliasName1;

  //cout<<joinedName<<endl;

  joinMap.insert(pair<string,AndList*>(joinedName,andlist));

  //print the map
  /*
     JoinMap::iterator it;
     for(it = joinMap.begin();it!=joinMap.end();it++){
     cout<<"Joined name: "<<it->first<<"  AndList: ";
     PrintAndList(it->second);
     cout<<endl;
     }
     */

}
void QueryOptimizer::scanJoins(){
  AndList* andlist = boolean;

  while(andlist!=NULL){
    OrList *orlist = andlist->left;
    while(orlist!=NULL){
      if(orlist->left->left->code == NAME && orlist->left->right->code== NAME){
	//it is a join
	populateJoinMap(orlist);
      }
      orlist = orlist->rightOr;
    }

    andlist = andlist->rightAnd;
  }
}

//righttableName should actually be lefttablename
//not handling the case where two relations can be joined on multiple attributes
AndList* QueryOptimizer::getAndList(vector<string> leftTableAliasVector,string rightTableName){
  cout<<"Printing info from getAndList\n";
  AndList* andlist = new AndList;
  andlist = NULL;
  for(int i=0;i<leftTableAliasVector.size();i++){
    string key="";
    if(rightTableName.compare(leftTableAliasVector[i])<=0){
      key+=rightTableName+"."+leftTableAliasVector[i];
    }else{
      key+=leftTableAliasVector[i]+"."+rightTableName;
    }

    //look up in the join map
    JoinMap::iterator it;
    it = joinMap.find(key);
    if(it!=joinMap.end()){
      //if found then add it to the right of the andlist array
      while(andlist!=NULL)
	andlist = andlist->rightAnd;
      andlist = it->second;
      break;
    }
  }

  //printing the andlist produced
  //if(andlist!=NULL){
  //	PrintAndList(andlist);
  //}
  //cout<<endl;
  return andlist;
}


AndList* QueryOptimizer::getAndListForRealJoin(vector<string> leftTableAliasVector,string rightTableName){
  cout<<"Printing info from getAndList\n";
  AndList* andList=NULL;
  AndList* ret=NULL;
  for(int i=0;i<leftTableAliasVector.size();i++){
    string key="";
    if(rightTableName.compare(leftTableAliasVector[i])<=0){
      key+=rightTableName+"."+leftTableAliasVector[i];
    }else{
      key+=leftTableAliasVector[i]+"."+rightTableName;
    }

    //look up in the join map
    JoinMap::iterator it;
    it = joinMap.find(key);
    if(it!=joinMap.end()){
      //if found then add it to the right of the andlist array
      if(andList==NULL){
	andList = it->second;
	ret = it->second;
      }else {
	andList->rightAnd = it->second;
	andList = andList->rightAnd;
      }
    }
  }

  //printing the andlist produced
  andList = ret;
  if(andList!=NULL){
    PrintAndList(andList);
  }

  cout<<endl;
  return ret;
}
vector<string> splitByDot(string name){
  vector<string> tokens;
  while(name.compare("")!=0){
    size_t dotpos = name.find(".");
    if(dotpos==string::npos){
      tokens.push_back(name);
      break;
    }
    tokens.push_back(name.substr(0,dotpos));
    name = name.substr(dotpos+1);
  }
  return tokens;
}

AndList* QueryOptimizer::findJoinSelect(string name){
  //tokenize into vector of string
  AndList* andList=NULL;
  AndList* ret=NULL;
  vector<string> tokensJoinOrder;
  tokensJoinOrder = splitByDot(name);
  string firstTable = tokensJoinOrder[0];

  SelectMap::iterator it;

  for(it=joinSelectMap.begin();it!=joinSelectMap.end();it++){
    vector<string> tokensJoinSelectMap;
    tokensJoinSelectMap = splitByDot(it->first);
    bool isValid = false;
    for(int i=0;i<tokensJoinSelectMap.size();i++){
      if(tokensJoinSelectMap[i].compare(firstTable)==0){
	isValid = true;
	break;
      }
    }

    if(!isValid)
      return NULL;

    //now check each element of token select map if present in token join order

    bool match = false;
    for(int i=0;i<tokensJoinSelectMap.size();i++){
      string selectString = tokensJoinSelectMap[i];
      match = false;
      for(int j=0;j<tokensJoinOrder.size();j++){
	if(tokensJoinOrder[j].compare(selectString)==0){
	  match = true;
	  break;
	}
      }
      if(!match)
	break;

    }

    if(match==true){
      if(andList==NULL){
	andList = it->second;
	ret = it->second;
      }else {
	andList->rightAnd = it->second;
	andList = andList->rightAnd;
      }

    }


  }

  return ret;


  /*/print tokens
    cout<<"\n\nPrinting information from findJoinSelct\n";
    for(int i=0;i<tokens.size();i++){
    cout<<tokens[i]<<" ";
    }
    cout<<endl;
    */
}

//handle unsigned long thingy
string QueryOptimizer::computeOptimalJoins(vector<string> aliases,Statistics &stats){

  if(aliases.size()==1){
    return aliases[0];
  }




  double minCost = 100000000000000000000;
  string minString="Z.Z.Z.Z.Z.Z.Z.Z";
  AndList* minAndList = NULL;

  for(int i=0;i<aliases.size();i++){
    string leftTableName = aliases[i];
    string rightTableName="";
    vector<string> newAliases;
    for(int j=0;j<aliases.size();j++){
      if(i==j)
	continue;
      //leftTableName+=aliases[j]+".";
      newAliases.push_back(aliases[j]);
    }


    Statistics s(stats);
    rightTableName = computeOptimalJoins(newAliases,s);
    s.Write("testStatsFile.txt");

    //***************************************
    // compute the selection estimates, add them to cost and apply it

    // search left and right table in selection maps

    // first deal with the left, which would always be one (left join)


    SelectMap::iterator selitr;
    selitr = selmap.find(leftTableName);

    long double cost = 0.0;

    //applying the estimate on the left table name (eg l , c, o )
    if(selitr!=selmap.end()){
      //apply and estimate but dont add this in the cost (left deep case)
      AndList *selectAndList = selmap[leftTableName];
      cout<<"\nPerforming a selection on "<<leftTableName<<endl;
      PrintAndList(selectAndList); cout<<endl;
      char* relNameForSelect[1];
      relNameForSelect[0]= (char*) leftTableName.c_str();
      cost+=s.Estimate(selectAndList,relNameForSelect,1);
      s.Apply(selectAndList,relNameForSelect,1);
    }

    //now apply it on the right tablename eg ( l.o , o.c etc)


    //looking in simple select map
    if(aliases.size()==2){
      selitr = selmap.find(rightTableName);
      if(selitr!=selmap.end()){
	AndList *selectAndList = selmap[rightTableName];
	cout<<"\nPerforming a selection on "<<rightTableName<<endl;
	PrintAndList(selectAndList); cout<<endl;
	char* relNameForSelect[1];
	relNameForSelect[0] = (char*) rightTableName.c_str();
	cost+=s.Estimate(selectAndList,relNameForSelect,1);
	s.Apply(selectAndList,relNameForSelect,1);
      }
    }else{
      //the case where we have to look up selection on joins
      //also add it to the cost

      //tokenize the right string
      AndList* splSelectAndList = findJoinSelect(rightTableName);


      if(splSelectAndList != NULL){
	cout<<"\nPerforming a selection on "<<rightTableName<<endl;
	PrintAndList(splSelectAndList); cout<<endl;
	char *relationNames[aliases.size()-1];
	for(int i=0;i<aliases.size();i++){
	  relationNames[i] = (char*)aliases[i].c_str();
	}
	cost+= s.Estimate(splSelectAndList,relationNames,aliases.size()-1);
	s.Apply(splSelectAndList,relationNames,aliases.size()-1);
      }


    }



    //*************************
    //get the andlist for the join, useful in estimation
    AndList* andlist = getAndList(newAliases,leftTableName);
    cout<<"Andlist found is as follows :-\n";
    PrintAndList(andlist); cout<<endl;
    char** relNames;
    int size = 0;
    if(andlist==NULL){
      cout<<"AndList is NULL so creating relnames accordingly\n";
      relNames = new char*[2];
      relNames[0] = (char*)leftTableName.c_str();
      relNames[1] = (char*)rightTableName.c_str();
      size=2;
    }else{

      relNames = new char*[aliases.size()];
      for(int i=0;i<aliases.size();i++){
	relNames[i] = (char*)aliases[i].c_str();
      }
      size = aliases.size();
    }


    cout<<"Relation names are as follows:-\n";
    for(int i=0;i<size;i++){
      cout<<relNames[i]<<" ";
    }

    //cout<<"\nCalling estimation function\n";
    //*******************************************



    cost += s.Estimate(andlist,relNames,aliases.size());
    //cout<< " VALUE of cost before apply is = "<< cost<<endl;
    s.Apply(andlist,relNames,aliases.size());

    string joinedName = leftTableName+"."+rightTableName;
    cout<<"Joined Name: "<<joinedName<<endl;

    if(minCost>cost){
      minCost = cost;
      minString = joinedName;
      minAndList = andlist;
      s.Write("minstatfile.txt");
      //while(1);
    }


    //put this and cost into the cost map

    costMap.insert(pair<string,double>(joinedName,cost));

  }

  //stats.Apply(minAndList,relNames,aliases.size());
  cout<<"\n\n******VALUE of MINIMUM cost at this level is "<<minCost<<endl;
  cout<<"******VALUE of MINIMUM String at this level is "<<minString<<endl<<endl;

  stats.Read("minstatfile.txt");
  stats.Write("testStatWrite.txt");
  return minString;



}
void QueryOptimizer::EnumerateAllPossibleJoins(){
  TableMap::iterator it;
  vector<string> aliases;
  for(it=aliasToTableMap.begin();it!=aliasToTableMap.end();it++){
    aliases.push_back(it->first);
  }

  Statistics stats(statistics);
  //stats.Write("Statistics.txt");
  optimalJoinOrder = computeOptimalJoins(aliases,stats);
  cout<<"My order is : "<< optimalJoinOrder <<endl;

  //printing map

  JoinCostMap::iterator itr;

  for(itr=costMap.begin();itr!=costMap.end();itr++){
    cout<<"Order: "<<itr->first<<"  Cost: "<<itr->second<<endl;
  }
}

TreeNode* QueryOptimizer::createSelectNode(string tableAliasName){
  string tableName = aliasToTableMap[tableAliasName];
  //cout<<"Table Name is : "<<tableName<<endl;

  SelectMap::iterator selItr;
  selItr = selmap.find(tableAliasName);
  AndList* andlist;
  Record *literal = new Record;
  CNF *cnf = new CNF;
  //cnf = NULL;
  Schema *schema = aliasToSchemaMap[tableAliasName];
  if(selItr!=selmap.end()){
    andlist = selmap[tableAliasName];
    if(andlist!=NULL){
      cnf->GrowFromParseTree(andlist,schema,*literal);
    }else{
      cnf = NULL;
    }
  }


  string filename = tableName + ".bin";
  Pipe *outPipe = new Pipe(pipe_size);
  TreeNode *node = new SelectFileNode(++nextPipeID,filename,cnf,literal,outPipe,schema);
  //nextPipeID++;
  //node->PrintNode();
  return node;

}


TreeNode* QueryOptimizer::createProjectNode(Schema* schema){
  vector<int> attsToKeep;
  int numAttOutput =0;

  NameList* projectAtts = attsToSelect;
  while(projectAtts!=NULL){
    numAttOutput++;
    int pos = schema->Find(projectAtts->name);
    if(pos==-1){
      cerr <<" Attribute to project Not Found in the schema\n";
      exit(1);
    }
    attsToKeep.push_back(pos);
    projectAtts = projectAtts->next;
  }

  int *keep = new int[numAttOutput];

  for(int i=0;i<numAttOutput;i++){
    keep[i] = attsToKeep[i];
  }

  int numAttsInput = schema->GetNumAtts();


  //make a new schema
  Attribute* outAtts = new Attribute[numAttOutput];
  projectAtts = attsToSelect;
  for(int i=0;i<numAttOutput;i++){
    outAtts[i].name = projectAtts->name;
    Type type = schema->FindType(projectAtts->name);
    outAtts[i].myType = type;
    projectAtts = projectAtts->next;
  }

  Schema* outSchema = new Schema("ProjectSchema",numAttOutput,outAtts);

  outSchema->PrintSchema();
  //while(1);
  Pipe *inPipe = root->outPipe;
  Pipe* outPipe = new Pipe(pipe_size);
  int inputPipeID = root->outPipeID;
  int outPipeID = ++nextPipeID;
  TreeNode* node = new ProjectNode(root,inputPipeID,inPipe,outPipeID,outPipe,keep,numAttsInput,numAttOutput,outSchema);
  node->leftChild = root;
  return node;
  //node->PrintNode();

}

TreeNode* QueryOptimizer::createJoinNode(TreeNode* left, TreeNode* right,string rightTableName){

  //get the andlist for the join first, needed to make the schema

  vector<string> tablesJoinedSoFar = splitByDot(joinedNameSoFar);
  AndList* andlist = getAndListForRealJoin(tablesJoinedSoFar,rightTableName);



  // update the joinedNameSOfar, needed to make the name for the joined schema
  joinedNameSoFar.append(".");
  joinedNameSoFar.append(rightTableName);



  //need a cnf, to make that you need schema, for schema u need num of atts left+right
  // and then array of atts left + right
  Schema *leftSchema = left->schema;
  Schema *rightSchema = right->schema;

  Attribute *leftAtts = leftSchema->GetAtts();
  Attribute *rightAtts = rightSchema->GetAtts();

  int totalAtts = leftSchema->GetNumAtts()+rightSchema->GetNumAtts();

  Attribute *joinedAtts = new Attribute[totalAtts];

  int i;
  for(i=0;i<leftSchema->GetNumAtts();i++){
    char* newAttName = new char[50];
    strcpy(newAttName,leftAtts[i].name);
    joinedAtts[i].name = newAttName;
    joinedAtts[i].myType = leftAtts[i].myType;
  }

  int j=0;
  for(;i<totalAtts;i++,j++){
    char* newAttName = new char[50];
    strcpy(newAttName,rightAtts[j].name);
    joinedAtts[i].name = newAttName;
    joinedAtts[i].myType = rightAtts[j].myType;
  }

  Schema *joinedSchema = new Schema((char*)joinedNameSoFar.c_str(),totalAtts,joinedAtts);
  //joinedSchema->PrintSchema();


  Record *literal = new Record;
  CNF* cnf = new CNF;
  cnf->GrowFromParseTree(andlist,leftSchema,rightSchema,*literal);

  int inPipe1ID = left->outPipeID;
  int inPipe2ID = right->outPipeID;
  int outPipeID = ++nextPipeID;
  Pipe* inPipeLeft = left->outPipe;
  Pipe* inPipeRight = right->outPipe;
  Pipe* outPipe = new Pipe(pipe_size);
  TreeNode* joinedNode = new JoinNode(left,right,inPipe1ID,inPipeLeft,inPipe2ID,inPipeRight,outPipeID,outPipe,cnf,literal,joinedSchema);


  return joinedNode;

}

TreeNode* QueryOptimizer::createSumNode(){

  //create function
  Function* function = new Function;
  function->GrowFromParseTree(finalFunction,*(root->schema));
  //function->Print();

  Attribute *sumAtt = new Attribute;
  if(function->returnsInt){
    sumAtt->myType = Int;
  }else{
    sumAtt->myType = Double;
  }
  char *summAttName = new char[50];
  strcpy(summAttName,"Sum");
  sumAtt->name = summAttName;
  Schema *schema = new Schema("sum_schema", 1, sumAtt);



  int inPipeID = root->outPipeID;
  int outPipeID = ++nextPipeID;
  Pipe* inPipe = root->outPipe;
  Pipe* outPipe = new Pipe(pipe_size);



  TreeNode* node = new SumNode(root,inPipeID,inPipe,outPipeID,outPipe,function,schema);

  return node;


}

TreeNode* QueryOptimizer::createSelectPipeNode(AndList* andlist){
  //need the cnf, for that need the schema which would be there in root anyways

  Schema* schema = root->schema;
  CNF *cnf = new CNF;
  Record* literal = new Record();
  cnf->GrowFromParseTree(andlist,schema,*literal);

  schema->PrintSchema();
  cnf->Print();
  int inPipeID = root->outPipeID;
  int outPipeID = ++nextPipeID;
  Pipe* inPipe = root->outPipe;
  Pipe* outPipe = new Pipe(pipe_size);

  TreeNode* node = new SelectPipeNode(root,inPipeID,inPipe,outPipeID,outPipe,cnf,literal,schema);

}

void QueryOptimizer::getOrderMaker(OrderMaker* ordermaker, Schema* schema){

  NameList *groupAtts = groupingAtts;
  while(groupAtts!=NULL){
    //make l.line from l_line

    int pos = schema->Find(groupAtts->name);
    Type type = schema->FindType(groupAtts->name);

    if(pos == -1 ){
      cout<<"\nERROR ! Can't create ordermaker for group by Did not find attribute in the schema\n";
    }
    ordermaker->whichAtts[ordermaker->numAtts] = pos;
    ordermaker->whichTypes[ordermaker->numAtts] = type;
    ordermaker->numAtts++;
    groupAtts = groupAtts->next;
  }


}

TreeNode* QueryOptimizer::createGroupByNode(){
  //make the ordermaker

  OrderMaker* ordermaker = new OrderMaker;
  Schema* schema = root->schema;
  getOrderMaker(ordermaker,schema);
  //ordermaker->Print();

  Function* function = new Function;
  function->GrowFromParseTree(finalFunction,*(root->schema));

  //make the output schema
  /*
     Attribute *sumAtt = new Attribute;
     if(function->returnsInt){
     sumAtt->myType = Int;
     }else{
     sumAtt->myType = Double;
     }
     */

  int numAtts = ordermaker->numAtts+1;
  Attribute *outAtts = new Attribute[numAtts];
  if(function->returnsInt){
    outAtts[0].myType = Int;
  }else{
    outAtts[0].myType = Double;
  }

  char *summAttName = new char[50];
  strcpy(summAttName,"Sum");
  outAtts[0].name = summAttName;


  //grouping atts into Attribute
  NameList *GroupAtts = groupingAtts;

  for(int i=1;i<numAtts;i++){
    outAtts[i].name = GroupAtts->name;
    Type type = schema->FindType(GroupAtts->name);
    outAtts[i].myType = type;

    GroupAtts = GroupAtts->next;
  }

  Schema *outSchema = new Schema("GroupByOutSchema",numAtts,outAtts);
  //outSchema->PrintSchema();

  int inPipeID = root->outPipeID;
  Pipe* inPipe  = root->outPipe;
  int outPipeID = ++nextPipeID;
  Pipe* outPipe = new Pipe(pipe_size);

  TreeNode* node = new GroupByNode(root,inPipeID,inPipe,outPipeID,outPipe,ordermaker,function,outSchema);


  return node;

}

TreeNode* QueryOptimizer::createDuplicateRemovalNode(){
  Schema* schema = root->schema;
  int inPipeID = root->outPipeID;
  Pipe* inPipe = root->outPipe;
  int outPipeID = ++nextPipeID;
  Pipe* outPipe = new Pipe(pipe_size);

  TreeNode* node = new DuplicateRemovalNode(root,inPipeID,inPipe,outPipeID,outPipe,schema);

  return node;

}

void QueryOptimizer::BuildTree(){
  cout<<"\n\nBuilding the Tree For Query Plan\n\n";
  vector<string> joinOrder = splitByDot(optimalJoinOrder);
  //joinedNameSoFar="";
  reverse(joinOrder.begin(),joinOrder.end());


  //no join
  if(joinOrder.size()==1){
    string tableAliasName = joinOrder[0];
    root = createSelectNode(tableAliasName);

    if(groupingAtts){
      root = createGroupByNode();
    }else{

      if(finalFunction){
	if(distinctFunc==0)
	  root= createSumNode();
      }else{

	if(attsToSelect!=NULL)
	  root = createProjectNode(root->schema);

	if(distinctAtts==1){
	  root = createDuplicateRemovalNode();
	}else if(distinctFunc==1){
	  cout<<"\nSUM DISTINCT To be handled...\n";
	}

      }
    }



  }else {
    //create the select for the leftmost table and then keep joining other tables
    string tableAliasName = joinOrder[0];
    root = createSelectNode(tableAliasName);
    joinedNameSoFar.append(tableAliasName);

    joinOrder.erase(joinOrder.begin(),joinOrder.begin()+1);

    while(joinOrder.size()>0){
      string aliasName = joinOrder[0];
      TreeNode* right = createSelectNode(aliasName);
      TreeNode* left = root;
      root = createJoinNode(left,right,aliasName);

      AndList *joinSelectAndList = findJoinSelect(joinedNameSoFar);
      if(joinSelectAndList!=NULL){
	PrintAndList(joinSelectAndList);
	root = createSelectPipeNode(joinSelectAndList);
      }
      //erase the joined table
      joinOrder.erase(joinOrder.begin(),joinOrder.begin()+1);

    }
    /*
       if(groupingAtts){
       root = createGroupByNode();
       }else if(finalFunction){
       root= createSumNode();
       }

       if(attsToSelect!=NULL)
    //root = createProjectNode(root->schema);

    if(distinctAtts==1){
    root = createDuplicateRemovalNode();
    }else if(distinctFunc==1){
    cout<<"\nSUM DISTINCT To be handled...\n";
    }

*/

    if(groupingAtts){
      root = createGroupByNode();
    }else{

      if(finalFunction){
	if(distinctFunc==0)
	  root= createSumNode();
      }else{

	if(attsToSelect!=NULL)
	  root = createProjectNode(root->schema);

	if(distinctAtts==1){
	  root = createDuplicateRemovalNode();
	}else if(distinctFunc==1){
	  cout<<"\nSUM DISTINCT To be handled...\n";
	}

      }
    }




  }




}



void QueryOptimizer::PrintQueryPlan(){
  cout<<"\n\n\n******************QUERY PLAN*******************\n\n";

  if(root!=NULL)
    root->PrintNode();
}

void QueryOptimizer::ExecuteTree(int choice){
  root->ExecuteNode();

  //test execution select+project+join
  //root->schema->PrintSchema();

  if(choice == 1){
    Record rec;
    int count = 0;
    Pipe *testOutputPipe = root->outPipe;
    root->schema->PrintSchema();
    while (testOutputPipe->Remove (&rec))
    {
      rec.Print(root->schema);
      count++;
    }



    cout << endl << count << " records removed from pipe " << endl;

  }else if(choice==2){
    //output in file

    string fileName;
    ifstream infile("OUTPUTFLAG");
    getline(infile,fileName);
    FILE* file = fopen(fileName.c_str(),"w");
    WriteOut W;
    W.Run(*(root->outPipe),file,*(root->schema));
    W.WaitUntilDone();
  }
}
