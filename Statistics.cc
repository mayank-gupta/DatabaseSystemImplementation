#include "Statistics.h"
#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <math.h>
Statistics::Statistics(){

}
Statistics::Statistics(Statistics &copyMe){
  RelationMap::iterator it;

  for(it= copyMe.relations.begin();it!=copyMe.relations.end();it++){
    //extract name of the relation
    const char* relName;
    relName = it->first;

    //make the relationcontainer object
    RelationContainer relcont;
    //hashmap for the container
    AttributeMap newAttributes(it->second.attributes);;
    //add number of tuples
    relcont.numberOfTuples = it->second.numberOfTuples;
    //assign hashmap , dont know if it will work or not
    relcont.attributes = newAttributes;

    this->relations[relName] = relcont;
  }

}

Statistics::Statistics(const Statistics &copy){
  RelationMap::iterator it;

  Statistics copyMe = (Statistics) copy;
  for(it= copyMe.relations.begin();it!=copyMe.relations.end();it++){
    //extract name of the relation
    const char* relName;
    relName = it->first;

    //make the relationcontainer object
    RelationContainer relcont;
    //hashmap for the container
    AttributeMap newAttributes(it->second.attributes);;
    //add number of tuples
    relcont.numberOfTuples = it->second.numberOfTuples;
    //assign hashmap , dont know if it will work or not
    relcont.attributes = newAttributes;

    this->relations[relName] = relcont;
  }

}

Statistics::~Statistics()
{
  destroyHashMap();
}

void Statistics::destroyHashMap(){
  RelationMap::iterator it;
  for(it= relations.begin();it!=relations.end();it++){
    it->second.attributes.clear();
  }

  relations.clear();
}

void Statistics::AddRel(char *relName, int numTuples)
{
  RelationContainer relContainer;
  relContainer.numberOfTuples = numTuples;
  relations[relName] = relContainer;

  //cout<<"Relation added successfully\n";

  //cout<<relations.begin()->first<<" "<<relations.begin()->second.numberOfTuples<<endl;
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
  RelationMap::iterator it;

  it = relations.find(relName);
  if(it == relations.end()){
    cout<<"could not add attribute into relations\n";
  }

  it->second.attributes.insert(pair<const char*,int>(attName,numDistincts));

  //cout<<"Attribute added successfully\n";
  //cout<<relations.begin()->first<<" "<<relations.begin()->second.numberOfTuples<<"   "<<relations.begin()->second.attributes.begin()->first<<" "<<relations.begin()->second.attributes.begin()->second<<endl;
}
void Statistics::CopyRel(char *oldName, char *newName)
{
  //hash_map<const char*,int,hash<const char*>,EqualString> newAttributes(relations[oldName].attributes);
  RelationContainer relcont;
  relcont.numberOfTuples = relations[oldName].numberOfTuples;


  AttributeMap::iterator attrit;
  for(attrit= relations[oldName].attributes.begin();attrit!=relations[oldName].attributes.end();attrit++){
    char *newAttName = new char[50];
    strcpy(newAttName,newName);
    strcat(newAttName,".");
    strcat(newAttName,attrit->first);
    //cout<<endl<<newAttName<<endl;
    relcont.attributes[newAttName] = attrit->second;
  }


  //relcont.attributes = newAttributes;
  relations[newName] = relcont;

  //hash_map<const char*,RelationContainer,hash<const char*>,EqualString>::iterator it;
  //it = relations.begin();
  //cout<<it->first<<" "<<it->second.numberOfTuples<<endl;
  //cout<<it->first<<" "<<it->second.numberOfTuples<<"   "<<it->second.attributes.begin()->first<<" "<<it->second.attributes.begin()->second<<endl;

  //it++;


  //cout<<it->first<<" "<<it->second.numberOfTuples<<endl;
  //cout<<it->first<<" "<<it->second.numberOfTuples<<"   "<<it->second.attributes.begin()->first<<" "<<it->second.attributes.begin()->second<<endl;


}

void Statistics::Read(char *fromWhere){
  relations.clear();
  ifstream in;
  in.open(fromWhere);
  if(in.is_open()){
    long int n;
    in >> n;

    for(int count = 0;count<n;count++){
      char *relname = new char[100];
      in >> relname;

      RelationContainer *relcont = new RelationContainer;

      in >> relcont->numberOfTuples;

      int numOfAtts;

      in >> numOfAtts;
      long int numOfDistincts;
      for(int i=0;i<numOfAtts;i++){
	char* attName = new char[50];
	in >> attName;
	in >> numOfDistincts;
	relcont->attributes[attName] = numOfDistincts;
      }


      relations[relname] = *relcont;

    }

    in.close();

  }




}
void Statistics::Write(char *fromWhere)
{

  //cout<<"\nWriting this information to the file\n\n";
  //consoleWrite("blah");
  ofstream out;
  out.open(fromWhere);

  RelationMap::iterator it;
  AttributeMap::iterator attrit;

  out<<relations.size()<<endl;
  for(it= relations.begin();it!=relations.end();it++){
    char temp[100];
    sprintf(temp,"%ld",it->second.numberOfTuples);
    //cout<<temp;
    out<< it->first<<" " <<temp<< endl;
    //cout<< it->first<<" " <<temp<< endl;
    out<<it->second.attributes.size() <<endl;
    //cout<<it->second.attributes.size() <<endl;

    for(attrit = it->second.attributes.begin();attrit!=it->second.attributes.end();attrit++){
      char temp1[100];
      sprintf(temp1,"%ld",attrit->second);
      out<<attrit->first<<" "<<temp1 <<" ";
      //cout<<attrit->first<<" "<<temp1 <<" ";
    }
    out <<endl;
    //cout<<endl;
  }
  out.close();
}

void Statistics::consoleWrite(char *fromWhere)
{
  //ofstream out;
  //out.open(fromWhere);

  RelationMap::iterator it;
  AttributeMap::iterator attrit;

  cout<<relations.size()<<endl;
  for(it= relations.begin();it!=relations.end();it++){
    cout<< it->first<<" " <<it->second.numberOfTuples << endl;
    cout<<it->second.attributes.size() <<endl;
    for(attrit = it->second.attributes.begin();attrit!=it->second.attributes.end();attrit++){
      cout<<attrit->first<<" "<<attrit->second <<" ";
    }
    cout <<endl;
  }
}


void Statistics::findTable(char* value, char **relNames, char* tableLeft,int len,int &numOfTuples, int &numOfDistincts){

  AttributeMap::iterator attrit;
  RelationMap::iterator it;
  bool found = false;
  //cout<<"\nPRINTING THE RELATIONS FROM STATISTICS\n";
  for(it = relations.begin();it!=relations.end();it++){
    //cout<<it->first<<endl;
    RelationContainer relcont = it->second;
    attrit = relcont.attributes.find(value);
    if(attrit!=relcont.attributes.end()){
      found=true;
      strcpy(tableLeft,it->first);
      numOfDistincts = attrit->second;
      numOfTuples = relcont.numberOfTuples;
      return;
    }

  }

  if(found == false){
    cout<<"Could not find the attribute in any of the tables Exiting\n";
    exit(1);
  }
}

double Statistics::solveInEquality(struct ComparisonOp *pCom,char *tableLeft,char *tableRight,int numOfDistinctsLeftAtt, int numOfDistinctsRightAtt,int numOfTuplesLeftTable,int numOfTuplesRightTable, char *resultantRelationName){
  //check the left and right thing again please
  //double result = (double) numOfTuplesLeftTable/3.0 ;
  //relations[tableLeft].numberOfTuples = (int) result;
  strcpy(resultantRelationName,tableLeft);
  return 3;
}

double Statistics::solveEquals(struct ComparisonOp *pCom,char *tableLeft,char *tableRight,int numOfDistinctsLeftAtt, int numOfDistinctsRightAtt,int numOfTuplesLeftTable,int numOfTuplesRightTable,char *resultantRelationName){
  if(pCom->left->code == NAME && pCom->right->code== NAME){


    if(strcmp(tableLeft,tableRight)!=0){
      //cout <<"Join\n";

      //make a new Realtion Name

      char dummyLeft[200];
      strcpy(dummyLeft,tableLeft);
      strcat(dummyLeft,".");
      strcpy(resultantRelationName,dummyLeft);
      strcat(resultantRelationName,tableRight);
      //cout<<resultantRelationName<<endl;

      //calcuate the resultant tuples and distincts
      unsigned long long a = numOfTuplesLeftTable;
      unsigned long long b = numOfTuplesRightTable;
      unsigned long long product = a*b;
      int resultingTuples = product/max(numOfDistinctsLeftAtt,numOfDistinctsRightAtt);
      //cout<<"Resulting tuples = "<<resultingTuples<<endl;
      int resultingDistincts = min(numOfDistinctsLeftAtt,numOfDistinctsRightAtt);
      //cout<<"Resulting Distincts = "<<resultingDistincts<<endl;

      //change the number of distincts in the hash table before merging
      relations[tableLeft].attributes[pCom->left->value] = resultingDistincts;
      relations[tableRight].attributes[pCom->right->value] = resultingDistincts;

      //construct the relation container for the new relation

      RelationContainer *relcont = new RelationContainer;
      relcont->numberOfTuples = resultingTuples;

      //attribute iterator
      AttributeMap::iterator attrit;

      RelationContainer *left = &relations[tableLeft];
      RelationContainer *right = &relations[tableRight];

      //add all the attributes from left table into the new relation
      for(attrit= left->attributes.begin();attrit!=left->attributes.end();attrit++){
	relcont->attributes[attrit->first] = attrit->second;
      }

      //add all the attributes from the right table
      for(attrit= right->attributes.begin();attrit!=right->attributes.end();attrit++){
	relcont->attributes[attrit->first] = attrit->second;
      }

      //finally add the new relation to hash table

      relations[resultantRelationName] = *relcont;

      //now remove the left and right relations from the hash map

      relations.erase(tableLeft);
      relations.erase(tableRight);
      // if returned value is -10 then it was a join


      return -10;
    }

  }else {
    //***********	V V V IMPORTANT 	****************
    //handle when right operand can be an attr name
    if(pCom->right->code == NAME){
      Operand* temp = pCom->left;
      pCom->left = pCom->right;
      pCom->right = temp;

      // like this you will have to swap everything
    }

    // now you have one attr and equal sign and other is watever in the right side
    //double a = numOfTuplesLeftTable/1.0;
    //double b = numOfDistinctsLeftAtt/1.0;
    //double result = a/b;
    //relations[tableLeft].numberOfTuples = (int) result;
    //crelations[tableLeft].attributes[pCom->left->value] = 1;
    strcpy(resultantRelationName,tableLeft);

    //cout<<endl<<"Result from inside solve equal = "<<result<<endl;
    return numOfDistinctsLeftAtt;
    //return 1.0/(numOfDistinctsLeftAtt);

  }
}

double Statistics::solveComparisonOp(struct ComparisonOp *pCom,char **relNames,int numToJoin, char* relationName){
  char tableLeft[200]="";
  char tableRight[200]="";
  int numOfTuplesLeftTable = -1,numOfTuplesRightTable = -1;
  int numOfDistinctsLeftAtt = -1,numOfDistinctsRightAtt = -1;

  if(pCom!=NULL)
  {
    //printf("%d ",pCom->left->code);
    // deal with left operand

    if(pCom->left->code == NAME){
      //search in hash table for left operand
      findTable(pCom->left->value,relNames,tableLeft,numToJoin,numOfTuplesLeftTable,numOfDistinctsLeftAtt);
      //cout<<"Table: "<<tableLeft<<" Tuples= "<<numOfTuplesLeftTable<<" Distinct= "<<numOfDistinctsLeftAtt<<endl;
    }

    if(pCom->right->code == NAME){
      //search in hash table for right operand
      findTable(pCom->right->value,relNames,tableRight,numToJoin,numOfTuplesRightTable,numOfDistinctsRightAtt);
      //cout<<"Table: "<<tableRight<<" Tuples= "<<numOfTuplesRightTable<<" Distinct= "<<numOfDistinctsRightAtt<<endl;
    }


    switch(pCom->code)
    {
      case GREATER_THAN:
	return solveInEquality(pCom,tableLeft,tableRight,numOfDistinctsLeftAtt,numOfDistinctsRightAtt,numOfTuplesLeftTable,numOfTuplesRightTable,relationName);
      case LESS_THAN:
	return solveInEquality(pCom,tableLeft,tableRight,numOfDistinctsLeftAtt,numOfDistinctsRightAtt,numOfTuplesLeftTable,numOfTuplesRightTable,relationName);

      case EQUALS:{

		    double result = solveEquals(pCom,tableLeft,tableRight,numOfDistinctsLeftAtt,numOfDistinctsRightAtt,numOfTuplesLeftTable,numOfTuplesRightTable,relationName);

		    return result;

		  }
    }


  }
  else
  {
    return NULL;
  }
}

bool checkIfOrListDependent(struct OrList* orList){
  bool dependent = true;
  //figure out if or list is independent or dependent

  struct ComparisonOp *compOp = orList->left;
  char prevAttr[200];
  if(compOp->left->code == NAME){
    strcpy(prevAttr,compOp->left->value);
  } else if(compOp->right->code == NAME){
    strcpy(prevAttr,compOp->right->value);
  }

  char currentAtt[200];

  while(orList->rightOr){
    orList = orList->rightOr;
    compOp = orList->left;
    if(compOp->left->code == NAME){
      strcpy(currentAtt,compOp->left->value);
    } else if(compOp->right->code == NAME){
      strcpy(currentAtt,compOp->right->value);
    }

    if(strcmp(currentAtt,prevAttr)!=0){
      dependent = false;
      break;
    }
  }

  return dependent;
}
double Statistics::solveOrList(struct OrList *pOr,char **relNames, int numToJoin, char *relationName){



  struct ComparisonOp *pCom;

  if(pOr !=NULL)
  {
    pCom = pOr->left;
    // find the total number of tuples before estimation
    int originalNumberOfTuples = 0;
    int numberOfDistincts=0;
    char tableLeft[200];
    char tableRight[200];

    if(pCom->left->code == NAME){
      //search in hash table for left operand
      findTable(pCom->left->value,relNames,tableLeft,numToJoin,originalNumberOfTuples,numberOfDistincts);
    }

    if(pCom->right->code == NAME){
      //search in hash table for right operand
      findTable(pCom->right->value,relNames,tableRight,numToJoin,originalNumberOfTuples,numberOfDistincts);
    }

    //cout<<"Table: "<<tableLeft<<" Original Tuples= "<<originalNumberOfTuples<<" Distinct= "<<numberOfDistincts<<endl;



    //case where only one expression in AND
    if(!(pOr->rightOr)){



      double result = solveComparisonOp(pCom,relNames,numToJoin,relationName);

      if(fabs(result+10.0) >0.1)
	return originalNumberOfTuples/result;
      else {
	//join

	return result;
      }
    }

    // else come to the other case with many OR's inside an AND
    bool dependent = checkIfOrListDependent(pOr);





    vector<double> results;
    results.push_back(solveComparisonOp(pCom,relNames,numToJoin,relationName));
    while(pOr->rightOr){
      pOr = pOr->rightOr;
      pCom = pOr->left;
      results.push_back(solveComparisonOp(pCom,relNames,numToJoin,relationName));

    }

    // by now all the results should be in vector, lets print them to see what is up
    //cout<<"Printing vector:-"<<endl;

    for(int i=0;i<results.size();i++){
      cout<<results[i]<<" ";

    }
    cout<<endl;

    if(dependent==true){
      double sum = 0.0;
      for(int i=0;i<results.size();i++){

	sum+=((double)(1.0*originalNumberOfTuples)/results[i]);
      }
      //printf("%f\n",sum);
      if(originalNumberOfTuples > (int)sum){
	return sum;
      }
      return originalNumberOfTuples;
    }else{
      double product = 1.0;
      for(int i=0;i<results.size();i++){
	product*= ( (results[i]-1)/results[i]);
      }
      //printf("Product : %f\n",product);

      double x = 1 - product;

      //printf("X : %f\n",x);

      x*=((double)originalNumberOfTuples*1.0);

      return x;
    }



  }
  else
  {
    return NULL;
  }
}

void Statistics::fixDistincts(const char *relationName){
  AttributeMap::iterator attrit;

  RelationContainer relcont = relations[relationName];
  for(attrit = relcont.attributes.begin();attrit!=relcont.attributes.end();attrit++){
    if(attrit->second > relcont.numberOfTuples){
      attrit->second = relcont.numberOfTuples;
    }
  }

}

double Statistics::solveAndList(struct AndList *pAnd, char **relNames, int numToJoin){
  double result = 0.0;
  char *relationName = new char[200];
  AttributeMap::iterator attrit;
  RelationMap::iterator it;
  if(pAnd !=NULL)
  {

    struct OrList *pOr = pAnd->left;
    result = solveOrList(pOr,relNames,numToJoin,relationName);
    //printf("Result after AND %f\n",result);
    //cout<<"Relation which would be updated now is "<<relationName<<endl;

    it = relations.find(relationName);
    if(fabs(result+10.0) >0.1){
      //cout<<"No Join\n";

      it->second.numberOfTuples=result;


    }

    while(pAnd->rightAnd)
    {
      pAnd = pAnd->rightAnd;
      pOr = pAnd->left;
      result = solveOrList(pOr,relNames,numToJoin,relationName);
      it = relations.find(relationName);
      if(fabs(result+10.0) >0.1){
	it->second.numberOfTuples=result;

      }
      //printf("Result after AND %f\n",result);

    }



    if(fabs(result+10.0) >0.1){
      cout<<"No join\n";
      fixDistincts(it->first);
      return result;
    }
    else{

      return relations[relationName].numberOfTuples;
    }
  }
  else
  {

    return NULL;

  }

}

long double Statistics::handleBlockNested(char** relNames){
  string leftTableName = relNames[0];
  string rightTableName = relNames[1];

  string joinedName = leftTableName+"."+rightTableName;
  char* joinName = strdup(joinedName.c_str());

  int numTuplesLeftTable = relations[(char*)leftTableName.c_str()].numberOfTuples;
  int numTuplesRightTable = relations[(char*)rightTableName.c_str()].numberOfTuples;

  long double resutingTuples = (long double)numTuplesLeftTable*(long double)numTuplesRightTable;
  //resutingTuples = 10000;
  RelationContainer *relcont = new RelationContainer;
  relcont->numberOfTuples = (unsigned long int) resutingTuples;

  AttributeMap::iterator attrit;
  RelationMap::iterator relit;


  relit = relations.find((char*)leftTableName.c_str());
  if(relit==relations.end()){
    cout<<"\n\n\nRelation not found in the statistics FILE. ERROR !!!\n\n";
    return NULL;
  }

  relit = relations.find((char*)rightTableName.c_str());

  if(relit==relations.end()){
    cout<<"\n\n\nRelation not found in the statistics FILE. ERROR !!!\n\n";
    return NULL;
  }

  RelationContainer *left = &relations[(char*)leftTableName.c_str()];
  RelationContainer *right = &relations[(char*)rightTableName.c_str()];

  //copy all the attributes

  for(attrit= left->attributes.begin();attrit!=left->attributes.end();attrit++){
    char *name = strdup(attrit->first);
    relcont->attributes[name] = attrit->second;
  }

  for(attrit= right->attributes.begin();attrit!=right->attributes.end();attrit++){
    char *name = strdup(attrit->first);
    relcont->attributes[name] = attrit->second;
  }


  //relations[(char*)joinedName.c_str()] = *relcont;
  relations.insert(pair<const char*,RelationContainer>(joinName,*relcont));

  //erase the old ones

  relations.erase((char*)leftTableName.c_str());
  relations.erase((char*) rightTableName.c_str());

  //cout<<"\n\n\nBLOCK NESTED\n\n";

  //cout<<"\n\n\nAfter updating the realtions \n\n\n";

  //consoleWrite("blah");


  return resutingTuples;

}

long double Statistics::Estimate(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  Statistics s(*this);

  if(parseTree==NULL){
    //cout<<"Block Nested Estimation\n";
    return s.handleBlockNested(relNames);
  }else
    return s.solveAndList(parseTree,relNames,numToJoin);


}



void  Statistics::Apply(struct AndList *parseTree, char **relNames, int numToJoin)
{
  if(parseTree==NULL){
    cout<<"\nBlock Nested Apply\n";
    handleBlockNested(relNames);
  }else
    solveAndList(parseTree,relNames,numToJoin);
}

