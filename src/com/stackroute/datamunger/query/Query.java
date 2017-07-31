package com.stackroute.datamunger.query;

import com.stackroute.datamunger.result.DataResultSetWithWhere;
import com.stackroute.datamunger.result.GroupByResultSet;
import com.stackroute.datamunger.result.OrderByResultSet;
import com.stackroute.datamunger.result.ResultSet;
import com.stackroute.datamunger.result.SimpleDataSetResult;

public class Query{

	private ResultSet resultSet;

	private QueryParser queryParser = new QueryParser();
	SimpleDataSetResult simpleDataSetResult;

	public ResultSet executeQuery(String query) {
		queryParser.parseQuery(query);
		resultSet = checkQueryForResult(query);
		return resultSet;
	}

	private ResultSet checkQueryForResult(String query) {

		if ( queryParser.isHasAggrigate()) {
			GroupByResultSet groupByResultSet = new GroupByResultSet();
			resultSet=groupByResultSet.analyzeQuery(queryParser);
		} else if (queryParser.isHasAggrigate()) {
			System.out.println("Aggregate without filter");
		} else if (queryParser.getOrderBy() != null) {
			OrderByResultSet orderByResultSet = new OrderByResultSet();
			resultSet=orderByResultSet.analyzeQuery(queryParser);
		} else if(queryParser.getWhereCondition() !=null){
			DataResultSetWithWhere dataResultWithWhere= new DataResultSetWithWhere();
			resultSet=dataResultWithWhere.analyzeQuery(queryParser);
		}
		else
		{
		    simpleDataSetResult=new SimpleDataSetResult();
			resultSet=simpleDataSetResult.analyzeQuery(queryParser);
		}
		return resultSet;
	}
}
