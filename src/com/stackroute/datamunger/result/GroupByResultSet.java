package com.stackroute.datamunger.result;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.stackroute.datamunger.analyzer.QueryAnalyzer;
import com.stackroute.datamunger.query.QueryParser;
import com.stackroute.datamunger.reader.CSVFileReader;
import com.stackroute.datamunger.restrictions.AggregateFunctions;
import com.stackroute.datamunger.restrictions.Restriction;

public class GroupByResultSet implements QueryAnalyzer {

	CSVFileReader csvFileReader;
	BufferedReader bufferedReader;
	ResultSet resultSet;
	List<String> groupByList;
	List<List<String>> groupBycolumnData;
	Set<String> uniqueSet;
	List<String> groupByData;
	Map<String, Integer> map;

	@Override
	public ResultSet analyzeQuery(QueryParser queryParser) {
		csvFileReader = new CSVFileReader(queryParser.getFilePath());

		/*
		 * if(queryParser.getGroupBy() !=null) {
		 * groupByList=Arrays.asList(queryParser.getGroupBy());
		 * groupBycolumnData=csvFileReader.getData(groupByList); uniqueSet=new
		 * LinkedHashSet<>(); groupByData= new ArrayList<>(); for(List<String>
		 * unique : groupBycolumnData) { uniqueSet.addAll(unique);
		 * groupByData.addAll(unique); } } int groupCount=0;
		 * 
		 * for(String uniqueValue : uniqueSet) { map = new HashMap<>();
		 * for(String value : groupByData ) {
		 * 
		 * if(uniqueValue.equals(value)) { groupCount++; } }
		 * map.put(uniqueValue, groupCount); groupCount=0;
		 * System.out.println(map); }
		 */
		int iteration = 0;

		resultSet = new ResultSet();
		List<AggregateFunctions> aggregateFunctions = queryParser.getAggregateFunction();
		for (AggregateFunctions aggregateFunction : aggregateFunctions) {
			String functionName = aggregateFunction.getFunctionName();
			String propertyName = aggregateFunction.getPropertyName();
			List<String> propertyList = Arrays.asList(propertyName);

			List<String> result=null;
			switch (functionName) {
			case "count":
				result = new ArrayList<>();
				result = aggregatePropertyCount(propertyList, queryParser);

				break;
			case "sum":
				result = new ArrayList<>();
				int sum = aggregatePropertySum(propertyList);
				result.add(String.valueOf(sum));
				break;
			case "avg":
				result = new ArrayList<>();
				int findSum = aggregatePropertySum(propertyList);
				List<String> avgCount = aggregatePropertyCount(propertyList,queryParser);
				result.add(String.valueOf(findSum / avgCount.size()));

				break;
			case "min":
				result = new ArrayList<>();
				int min = aggregatePropertyMin(propertyList);
				result.add(String.valueOf(min));

				break;
			case "max":
				result = new ArrayList<>();
				int max = aggregatePropertyMax(propertyList);
				result.add(String.valueOf(max));

				break;

			}
			resultSet.put(iteration++, result);

		}

		return resultSet;
	}

	private int aggregatePropertyMax(List<String> propertyList) {
		List<List<String>> dataSet = csvFileReader.getData(propertyList);
		int max = Integer.parseInt(dataSet.get(0).get(0));
		for (List<String> columnCount : dataSet) {
			if (Integer.parseInt(columnCount.get(0)) > max)
				max = Integer.parseInt(columnCount.get(0));
		}
		return max;
	}

	private int aggregatePropertyMin(List<String> propertyList) {
		List<List<String>> dataSet = csvFileReader.getData(propertyList);
		int min = Integer.parseInt(dataSet.get(0).get(0));
		for (List<String> columnCount : dataSet) {
			if (Integer.parseInt(columnCount.get(0)) < min)
				min = Integer.parseInt(columnCount.get(0));
		}
		return min;
	}

	private int aggregatePropertySum(List<String> propertyList) {
		List<List<String>> dataSet = csvFileReader.getData(propertyList);
		
		int sum = 0;
       
		for (List<String> columnCount : dataSet) {
			if (columnCount.get(0) != null) {
				sum = sum + Integer.parseInt(columnCount.get(0).trim());
			}
		}
		
		return sum;
		}
	

	private List<String> aggregatePropertyCount(List<String> propertyList, QueryParser queryParser) {
		List<String> countList = null;
		if (queryParser.getGroupBy() == null) {
			List<List<String>> dataSet = csvFileReader.getData(propertyList);
			countList = new ArrayList<>();
			int count = 0;
			for (List<String> columnCount : dataSet) {
				if (!columnCount.isEmpty())
					count++;
			}
			countList.add(String.valueOf(count));
			return countList;
		} else {
			groupByList = Arrays.asList(queryParser.getGroupBy());
			groupBycolumnData = csvFileReader.getData(groupByList);
			uniqueSet = new LinkedHashSet<>();
			groupByData = new ArrayList<>();
			for (List<String> unique : groupBycolumnData) {
				uniqueSet.addAll(unique);
				groupByData.addAll(unique);
			}

			int groupCount = 0;
			countList = new ArrayList<>();
			for (String uniqueValue : uniqueSet) {
				
				for (String value : groupByData) {

					if (uniqueValue.equals(value)) {
						groupCount++;
					}
				}
				String column = uniqueValue +" = "+ groupCount;
				groupCount=0;
				countList.add(column);

			}

		}
		return countList;
	}
}
