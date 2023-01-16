/**
*＠file sql5300.cpp - shell to execute SQL commsnds
*＠author Jeb-Chieh,Mohith 
*@see "Seattle University, cpsc5300, winter 2023"
*@Milestone1
*@Jan 16, 2023
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include <cassert>
#include "sqlhelper.h"
#include "SQLParser.h"
using namespace std;
using namespace hsql;

string unparseSelect(const SelectStatement* stmt);
string unparseCreate(const CreateStatement* stmt);
string unparseTable(const TableRef* table);
string unparseOperator( const Expr *expr);
string printExpression(const Expr *expr);



/**
*  unparse column type
**/
string columnDefinitionToString(const ColumnDefinition *col){
	string res(col->name);
	switch(col->type){
		case ColumnDefinition::DOUBLE:
			res += " DOUBLE";
			break;
		case ColumnDefinition::INT:
			res += " INT";
			break;
		case ColumnDefinition::TEXT:
			res += " TEXT";
			break;
		default:
		res += " ...";
		break;
	}
	return res;
}
/**
* unparse arithmetic and conditionals in SQL
**/
string unparseOperator(const Expr* expr){
	if(expr == NULL){
		return "null";
	}
	string res;
	res += printExpression(expr->expr) + " ";
	switch(expr->opType){
		case Expr::SIMPLE_OP:
			res += expr->opChar;
			break;
		case Expr::AND:
			res += "AND";
			break;
		case Expr::OR:
			res += "OR";
			break;
		case Expr::NOT:
			res += "NOT";
			break;
		case Expr::IN:
			res += "IN";
			break;
		case Expr::LIKE:
			res += "LIKE";
			break;			
		default:
			break;
	}
	if(expr->expr2 != NULL){
		res += " " + printExpression(expr->expr2);
	}
	return res;
}

/**
*  convert Abstract Syntax to string
**/
string printExpression(const Expr *expr){
	string res;

	switch(expr->type){
		case kExprStar:
			res += "*";
			break;
		case kExprColumnRef:
			if(expr->table != NULL){
				res += string(expr->table) + ".";
			}
		case kExprLiteralString:
			res += expr -> name;
			break;
		case kExprLiteralFloat:
			res += to_string(expr->fval);
			break;
		case kExprLiteralInt:
			res += to_string(expr->ival);
			break;
		
		case kExprOperator:
			res += unparseOperator(expr);
			break;
		case kExprFunctionRef:
			res += string(expr->name) + "?" + expr->expr->name;
			break;	
		default:
			res += "Invalid expression ";
			break;
	}
	if(expr->alias != NULL){
		res += string(" AS ") + expr->alias;
	} 
	return res;
} 

/**
* unparse join operators, table names and alias
**/
string unparseTable(const TableRef* table){
	string res;
	switch (table->type){
	case kTableSelect: 
		unparseSelect(table->select);
		break; 
	case kTableName: 
		res +=(table->name);
		if( table->alias != NULL){
			res += string(" AS ") + table->alias;
		}
		break; 
	case kTableJoin: 
		res += unparseTable(table->join->left);
		switch(table->join->type){
			
			case kJoinInner:
				res += " JOIN "; 
				break; 
			case kJoinOuter:
				res += " OUTER JOIN ";
				break;
			case kJoinLeft:
				res += " LEFT JOIN ";
				break;
			case kJoinRight:
				res += " RIGHT JOIN "; 
				break;	
			default:
				cout << "Invalid Join" << endl;
				break;
			} 
			res += unparseTable(table->join->right);
			if (table->join->condition != NULL){
				res += " ON " + unparseOperator(table->join->condition);
			}
		break;	
	case kTableCrossProduct:
		{
		bool columns = false;
		for(TableRef* tbl : *table->list) {
			if(columns){
				res += ", ";
			}
			res += unparseTable(tbl); 
			columns = true;
		}
		break;
		}
	default:
		cout << "Invalid Table Unparsing" << endl;
		break;
	}
	return res;
}


/**
*unparse Select SQL Statement
**/
string unparseSelect(const SelectStatement* stmt) {
	string res = "SELECT ";
	bool columns = false;
	for (Expr* expr : *stmt->selectList) {

		if(columns){
			res += ", ";
		}
		res += printExpression(expr);
		columns = true;
	}

	res += " FROM " + unparseTable(stmt->fromTable);

	if (stmt->whereClause != NULL){
		res += " WHERE " + printExpression(stmt->whereClause);
	}
	return res;
}

/**
*unparse CREATE SQL statement
**/
string unparseCreate(const CreateStatement* stmt){
	string res;
	res += "CREATE TABLE ";
	if(stmt->type != CreateStatement::kTable){
		return res + "Table is invalid";
	}
	if(stmt->ifNotExists){
		res += "IF NOT EXIST ";
	}
	res += string(stmt->tableName) + " (";
	bool columns = false;
	for (ColumnDefinition *col : *stmt->columns){
		if(columns){
			res += ", ";
		}
		res += columnDefinitionToString(col);
		columns = true;
	}
	res += ")";
	return res;
}

/**
* handle two different types of quary: Select and Create.
**/
string runsql(const SQLStatement* stmt) {

	if(stmt->type()==kStmtSelect)
		return unparseSelect((const SelectStatement*)stmt);
	else if(stmt->type()==kStmtCreate)	
	    return unparseCreate((const CreateStatement*)stmt);
	else
		return " Invalid sql statement" ;
}

int main(int argc, char **argv)
{
	//Check for columnsnd line paramenters if there are more than 1 paramenters.
	if (argc != 2) {
		cerr << "Usage: cpsc5300: dvenvpath" << endl;
		return 1;
	}

	//arg[1] as directory path
	char *envDir = argv[1];
	DbEnv *myEnv = new DbEnv(0U);
	
	//create database env if it doesn't exist
	try {
		myEnv->open(envDir, DB_CREATE | DB_INIT_MPOOL, 0);
	}
	catch (DbException &e) {
		std::cerr << "Error opening database"
				  << envDir << std::endl;
		std::cerr << e.what() << std::endl;
		exit(-1);
	}
	catch (std::exception &e) {
		std::cerr << "Error opening database"
				  << envDir << std::endl;
		std::cerr << e.what() << std::endl;
		exit(-1);
	}
	
	//SQL starts
	while (true) {
		string sqlcmd;
		cout << "SQL>";
		getline(cin, sqlcmd);

		if (sqlcmd == "quit") {
			break;
		}
		if (sqlcmd.length() < 1) {
			continue;
		}

		//uses hsql parser for input statement
		hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(sqlcmd);
		//Check to see if hyrise parse result is valid
		if (!result->isValid()) {
			cout << "Invalid SQL:" << sqlcmd << endl;
			continue;
		}
		else {
			for (uint i = 0; i < result->size(); i++) {
				cout << runsql(result->getStatement(i)) << endl;
			}
		}
	}
    return EXIT_SUCCESS;
}

