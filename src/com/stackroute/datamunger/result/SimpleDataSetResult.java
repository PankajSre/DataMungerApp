package com.stackroute.datamunger.result;

import java.util.List;

import com.stackroute.datamunger.analyzer.QueryAnalyzer;
import com.stackroute.datamunger.query.QueryParser;
import com.stackroute.datamunger.reader.CSVFileReader;

public class SimpleDataSetResult implements QueryAnalyzer {
 
	CSVFileReader csvFileReader;
	@Override
	public ResultSet analyzeQuery(QueryParser queryParser) {
	   csvFileReader= new CSVFileReader(queryParser.getFilePath());
		ResultSet resultSet= new ResultSet();
		int i=0;
		List<List<String>> dataSet=null;
		if(queryParser.isAllColumns())
		{
			dataSet=csvFileReader.getData(queryParser.getFields());
		for(List<String> columnData : dataSet) resultSet.put(i++, columnData);
		return resultSet;
		}
		else
		{
			 dataSet=csvFileReader.getData(queryParser.getFields());
			for(List<String> columnData : dataSet) resultSet.put(i++, columnData);
			return resultSet;
		}
	}

}
