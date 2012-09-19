
CC = g++ -w 

tag = -i

ifdef linux
tag = -n
endif

dbs.out : DBS.o y.tab.o lex.yy.o QueryOptimizer.o Statistics.o Schema.o Comparison.o Record.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o SelectFile.o SelectPipe.o Project.o Sum.o DuplicateRemoval.o WriteOut.o Join.o ComparisonEngine.o BigQ.o RelOp.o Function.o File.o Pipe.o
	$(CC) -o dbs.out DBS.o y.tab.o lex.yy.o QueryOptimizer.o Statistics.o Comparison.o Record.o Schema.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o SelectFile.o SelectPipe.o Project.o Sum.o DuplicateRemoval.o WriteOut.o Join.o ComparisonEngine.o BigQ.o RelOp.o Function.o File.o Pipe.o -lpthread
	
main : y.tab.o lex.yy.o main.o QueryOptimizer.o Statistics.o Schema.o Comparison.o Record.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o SelectFile.o SelectPipe.o Project.o Sum.o DuplicateRemoval.o WriteOut.o Join.o ComparisonEngine.o BigQ.o RelOp.o Function.o File.o Pipe.o
	$(CC) -o main y.tab.o lex.yy.o main.o QueryOptimizer.o Statistics.o Comparison.o Record.o Schema.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o SelectFile.o SelectPipe.o Project.o Sum.o DuplicateRemoval.o WriteOut.o Join.o ComparisonEngine.o BigQ.o RelOp.o Function.o File.o Pipe.o -lpthread
	
main.o : main.cc
	$(CC) -g -c main.cc
	
QueryOptimizer.o : QueryOptimizer.cc TreeNode.h
	$(CC) -g -c QueryOptimizer.cc
	
Statistics.o : Statistics.cc
	$(CC) -g -c Statistics.cc	
	
Schema.o: Schema.cc
	$(CC) -g -c Schema.cc	
	
Comparison.o : Comparison.cc
	$(CC) -g -c Comparison.cc	
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc		

GenericDBFile.o: GenericDBFile.cc
	$(CC) -g -c GenericDBFile.cc
	
HeapDBFile.o: HeapDBFile.cc	
	$(CC) -g -c HeapDBFile.cc
	
SortedDBFile.o: SortedDBFile.cc
	$(CC) -g -c SortedDBFile.cc	
		
Record.o: Record.cc
	$(CC) -g -c Record.cc
	
DBS.o : DBS.cc
	$(CC) -g -c DBS.cc
	
SelectFile.o: SelectFile.cc
	$(CC) -g -c SelectFile.cc	

SelectPipe.o: SelectPipe.cc
	$(CC) -g -c SelectPipe.cc
	
Project.o: Project.cc
	$(CC) -g -c Project.cc	
	
Sum.o: Sum.cc
	$(CC) -g -c Sum.cc
	
DuplicateRemoval.o: DuplicateRemoval.cc
	$(CC) -g -c DuplicateRemoval.cc

WriteOut.o: WriteOut.cc
	$(CC) -g -c WriteOut.cc
	
Join.o: Join.cc
	$(CC) -g -c Join.cc	

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc	

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc	
			
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
