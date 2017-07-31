package com.stackroute.datamunger.result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.stackroute.datamunger.analyzer.QueryAnalyzer;
import com.stackroute.datamunger.query.QueryParser;
import com.stackroute.datamunger.reader.CSVFileReader;

public class OrderByResultSet implements QueryAnalyzer {

	CSVFileReader csvFileReader;

	@Override
	public ResultSet analyzeQuery(QueryParser queryParser) {
		csvFileReader = new CSVFileReader(queryParser.getFilePath());
		List<String> orderByColumn = Arrays.asList(queryParser.getOrderBy());
		int orderByIndex = csvFileReader.getColumnIndex(orderByColumn).get(0);
		ResultSet resultSet = new ResultSet();
		System.out.println(queryParser.getFields()+" : "+orderByIndex);
		int i = 0;
		List<List<String>> dataSet = null;
		if (queryParser.isAllColumns() && queryParser.getFields().size()==1) {
			dataSet = csvFileReader.getData(queryParser.getFields());
			dataSet.sort(new Comparator<List<String>>() {
				@Override
				public int compare(List<String> o1, List<String> o2) {
					return o1.get(orderByIndex).compareTo(o2.get(orderByIndex));
				}
			});
			for (List<String> columnData : dataSet)
				resultSet.put(i++, columnData);
			return resultSet;
		} else {
			dataSet = csvFileReader.getData(queryParser.getFields());
			dataSet.sort(new Comparator<List<String>>() {
				@Override
				public int compare(List<String> o1, List<String> o2) {
					return o1.get(orderByIndex).compareTo(o2.get(orderByIndex));
				}
			});
			for (List<String> columnData : dataSet)
				resultSet.put(i++, columnData);
			return resultSet;
		}
	}
}
