package com.stackroute.datamunger.result;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stackroute.datamunger.analyzer.QueryAnalyzer;
import com.stackroute.datamunger.query.QueryParser;
import com.stackroute.datamunger.reader.CSVFileReader;
import com.stackroute.datamunger.restrictions.Restriction;

public class DataResultSetWithWhere implements QueryAnalyzer {

	CSVFileReader csvFileReader;
	BufferedReader bufferedReader;
	@Override
	public ResultSet analyzeQuery(QueryParser queryParser) {
		 csvFileReader= new CSVFileReader(queryParser.getFilePath());
		List<String> singleRow=null;
		int i=0;
		ResultSet resultSet = new ResultSet();
		try {
			 bufferedReader = new BufferedReader(new FileReader(queryParser.getFilePath()));
			bufferedReader.readLine();
			String line;
			while ((line =bufferedReader.readLine()) != null) {
				singleRow = Arrays.asList(line.split(","));
				
				
				if(rowMatchedWithCondition(singleRow,queryParser))
				{
					resultSet.put(i++,singleRow);
				}

			}
			bufferedReader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	return resultSet;

}
public List<String> selectRequiredDataSet(List<String> rowData,QueryParser queryParser) {
		
		List<String> selectedFields= queryParser.getFields();
		List<String> rowRecord = new ArrayList<String>();
		
		for (String field : selectedFields) {
			rowRecord.add(field);
		}
		
		return rowRecord;

	}
	
	public boolean rowMatchedWithCondition(List<String> record,QueryParser queryParser) {
		List<Restriction> restrictions = queryParser.getRestrictionList();
		List<String> logicationOperators = queryParser.getLogicalOperators();
		boolean status = false;
		int expressionPosotion = 0;
		if (restrictions != null && !restrictions.isEmpty()) {
			
			Restriction restriction = restrictions.get(expressionPosotion);
			status = checkForlogicalExpressions(record, restriction);

			for (String logicationOperator : logicationOperators) {
				expressionPosotion++;
				switch (logicationOperator) {
				case "and":
					restriction = restrictions.get(expressionPosotion);
					status = status && checkForlogicalExpressions(record, restriction);
					break;
				case "or":
					restriction = restrictions.get(expressionPosotion);
					status = status || checkForlogicalExpressions(record, restriction);
					break;
				case "not":
					restriction = restrictions.get(expressionPosotion);
					status = status && !checkForlogicalExpressions(record, restriction);
				}
			}

		}
		return status;

	}

	private boolean checkForlogicalExpressions(List<String> rowSet, Restriction restriction) {
		String operator = restriction.getConditionalOperator();
		String propertyName = restriction.getPropertyName();
        List<String> header=csvFileReader.getHeader();
		String propertyValue = restriction.getPropertyValue();
		String recordValue = rowSet.get(header.indexOf(propertyName));
		boolean status = true;
		switch (operator.trim()) {
		case "=":
			if (propertyValue.equals(recordValue)) {
				status = status && true;
			} else {
				status = status && false;
			}
			break;
		case "!=":

			if (!propertyValue.equals(recordValue)) {
				status = status && true;
			} else {
				status = status && false;
			}
			break;

		case ">":

			if (Integer.parseInt(recordValue) > Integer.parseInt(propertyValue)) {
				status = status && true;
			} else {
				status = status && false;
			}

			break;

		case "<":

			if (Integer.parseInt(recordValue) < Integer.parseInt(propertyValue)) {
				status = status && true;
			} else {
				status = status && false;
			}
			break;
		case "<=":

			if (Integer.parseInt(recordValue) <= Integer.parseInt(propertyValue)) {
				status = status && true;
			} else {
				status = status && false;
			}
			break;
		case ">=":

			if (Integer.parseInt(recordValue) >= Integer.parseInt(propertyValue)) {
				status = status && true;
			} else {
				status = status && false;
			}
		
		}

		return status;
	}

}

