#define BOOST_HAS_HASH

#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <hash_map>
#include <cstring>


using namespace std;
using namespace __gnu_cxx;

struct EqualString{
  bool operator()(const char* s1, const char* s2) const {
    return strcmp(s1,s2)==0;
  }
};
typedef hash_map<const char*,unsigned long int,hash<const char*>,EqualString> AttributeMap;
class RelationContainer{
  public:
    unsigned long int numberOfTuples;
    AttributeMap attributes;
    RelationContainer(){
      numberOfTuples = 0;

    }


};


typedef hash_map<const char*,RelationContainer,hash<const char*>,EqualString> RelationMap;

class Statistics
{
  private:
  public:

    RelationMap relations;

    Statistics();
    Statistics(Statistics &copyMe);	 // Performs deep copy
    Statistics(const Statistics &copyMe);	 // Performs deep copy

    ~Statistics();
    void destroyHashMap();


    void AddRel(char *relName, int numTuples);
    void AddAtt(char *relName, char *attName, int numDistincts);
    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);
    void Write(char *fromWhere);

    void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    long double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
    double solveAndList(struct AndList *parseTree, char **relNames, int numToJoin);
    double solveComparisonOp(struct ComparisonOp *pCom,char **relNames, int numToJoin,char* relationName);
    double solveOrList(struct OrList *pOr,char **relNames, int numToJoin,char* relationName);
    void findTable(char* value, char ** relNames, char* tableLeft, int numToJoin,int &numOfTuples, int &numOfDistincts);
    double solveEquals(struct ComparisonOp *pCom,char *tableLeft,char *tableRight,int numOfDistinctsLeftAtt, int numOfDistinctsRightAtt,int numOfTuplesLeftTable,int numOfTuplesRightTable,char* s);
    double solveInEquality(struct ComparisonOp *pCom,char *tableLeft,char *tableRight,int numOfDistinctsLeftAtt, int numOfDistinctsRightAtt,int numOfTuplesLeftTable,int numOfTuplesRightTable,char* s);
    void consoleWrite(char *fromWhere);
    long double handleBlockNested(char** relNames);
    void fixDistincts(const char*);
};

#endif
