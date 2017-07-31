package com.stackroute.datamunger.result;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.stackroute.datamunger.analyzer.QueryAnalyzer;
import com.stackroute.datamunger.query.QueryParser;
import com.stackroute.datamunger.reader.CSVFileReader;
import com.stackroute.datamunger.restrictions.AggregateFunctions;

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

		
		  if(queryParser.getGroupBy() !=null) {
		  groupByList=Arrays.asList(queryParser.getGroupBy());
		  groupBycolumnData=csvFileReader.getData(groupByList); uniqueSet=new
		  LinkedHashSet<>(); groupByData= new ArrayList<>(); for(List<String>
		  unique : groupBycolumnData) { uniqueSet.addAll(unique);
		  groupByData.addAll(unique); } } 
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
				result = aggregatePropertySum(propertyList,queryParser);
				break;
			case "avg":
				result = new ArrayList<>();
				List<String> findSum = aggregatePropertySum(propertyList,queryParser);
				List<String> avgCount = aggregatePropertyCount(propertyList,queryParser);
				result.add(String.valueOf(findSum.size() / avgCount.size()));

				break;
			case "min":
				result = new ArrayList<>();
				result = aggregatePropertyMin(propertyList,queryParser);

				break;
			case "max":
				result = new ArrayList<>();
				 result = aggregatePropertyMax(propertyList,queryParser);
				break;

			}
			resultSet.put(iteration++, result);

		}

		return resultSet;
	}

	private List<String> aggregatePropertyMax(List<String> propertyList,QueryParser queryParser) {
		List<String> maxList=null;
		List<List<String>> dataSet = csvFileReader.getData(propertyList);
		if(queryParser.getGroupBy()==null)
		{
			maxList= new ArrayList<>();
		int min = Integer.parseInt(dataSet.get(0).get(0));
		for (List<String> columnCount : dataSet) {
			if (Integer.parseInt(columnCount.get(0)) < min)
				min = Integer.parseInt(columnCount.get(0));
		}
		maxList.add(String.valueOf(min));
		}else {
			
			int i=0;
			maxList = new ArrayList<>();
			for (String uniqueValue : uniqueSet) {
				int maxCount = Integer.parseInt(dataSet.get(i).get(0));
				for (String value : groupByData) {
					if (uniqueValue.equals(value)) {
						if(maxCount<Integer.parseInt(dataSet.get(i).get(0)))
							maxCount=Integer.parseInt(dataSet.get(i).get(0));
						
					}
				
				}
				i++;
				String column = uniqueValue +" = "+ maxCount;
				maxCount=0;
				maxList.add(column);
			}
		}
		return maxList;
	}

	private List<String> aggregatePropertyMin(List<String> propertyList,QueryParser queryParser) {
		List<String> minList=null;
		List<List<String>> dataSet = csvFileReader.getData(propertyList);
		if(queryParser.getGroupBy()==null)
		{
			minList= new ArrayList<>();
		int min = Integer.parseInt(dataSet.get(0).get(0));
		for (List<String> columnCount : dataSet) {
			if (Integer.parseInt(columnCount.get(0)) < min)
				min = Integer.parseInt(columnCount.get(0));
		}
		minList.add(String.valueOf(min));
		}else {
			
			int i=0;
			minList = new ArrayList<>();
			for (String uniqueValue : uniqueSet) {
				int minCount = Integer.parseInt(dataSet.get(i).get(0));
				for (String value : groupByData) {
					if (uniqueValue.equals(value)) {
						if(minCount>Integer.parseInt(dataSet.get(i).get(0)))
							minCount=Integer.parseInt(dataSet.get(i).get(0));
						
					}
				
				}
				i++;
				String column = uniqueValue +" = "+ minCount;
				minCount=0;
				minList.add(column);
			}
		}
		return minList;
	}

	private List<String> aggregatePropertySum(List<String> propertyList,QueryParser queryParser) {
		List<String> sumList=null;
		
		List<List<String>> dataSet = csvFileReader.getData(propertyList);
		if(queryParser.getGroupBy()==null)
		{
		int sum = 0;
        sumList= new ArrayList<>();
		for (List<String> columnCount : dataSet) {
			if (columnCount.get(0) != null) {
				sum = sum + Integer.parseInt(columnCount.get(0).trim());
			}
		}
		sumList.add(String.valueOf(sum));
		
		}else {
			int groupCount = 0;
			int i=0;
			sumList = new ArrayList<>();
			for (String uniqueValue : uniqueSet) {
				
				for (String value : groupByData) {
					if (uniqueValue.equals(value)) {
						groupCount=groupCount+Integer.parseInt(dataSet.get(i++).get(0));
					}
				}
				String column = uniqueValue +" = "+ groupCount;
				groupCount=0;
				sumList.add(column);
			}
		}
		
		return sumList;
		}
	

	private List<String> aggregatePropertyCount(List<String> propertyList, QueryParser queryParser) {
		List<String> countList = null;
		if (queryParser.getGroupBy() == null) {
			List<List<String>> dataSet = csvFileReader.getData(propertyList);
			System.out.println(dataSet.get(0).get(0));
			countList = new ArrayList<>();
			int count = 0;
			for (List<String> columnCount : dataSet) {
				if (!columnCount.isEmpty())
					count++;
			}
			countList.add(String.valueOf(count));
			return countList;
		} else {
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
