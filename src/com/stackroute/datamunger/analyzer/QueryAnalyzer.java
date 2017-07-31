package com.stackroute.datamunger.analyzer;
import com.stackroute.datamunger.query.QueryParser;
import com.stackroute.datamunger.result.ResultSet;

public interface QueryAnalyzer {

	public ResultSet analyzeQuery(QueryParser queryParser);
	
}
