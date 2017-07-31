package com.stackroute.datamunger.query;

import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.restrictions.AggregateFunctions;
import com.stackroute.datamunger.restrictions.Restriction;
import com.stackroute.datamunger.result.ResultSet;

public class QueryParser {
 ResultSet resultSet;
	public QueryParser() {
	 resultSet = new ResultSet();
	}
	private String groupBy;
	private String orderBy;
	private String filePath;
	private boolean allColumns;
	private List<AggregateFunctions> aggregateFunction= new ArrayList<>();
	private List<Restriction> restrictionList = new ArrayList<>();
	private String whereCondition;
	List<String> logicalOperators = new ArrayList<>();
	private boolean hasAggrigate = false;
	private List<String> fields = new ArrayList<>();
    private String queryType="Simple_Query";
	public ResultSet getResultSet() {
		return resultSet;
	}

	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public String getQueryType() {
		return queryType;
	}

	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}

	public List<AggregateFunctions> getAggregateFunction() {
		return aggregateFunction;
	}

	public void setAggregateFunction(List<AggregateFunctions> aggregateFunction) {
		this.aggregateFunction = aggregateFunction;
	}

	public boolean isHasAggrigate() {
		return hasAggrigate;
	}

	public void setHasAggrigate(boolean hasAggrigate) {
		this.hasAggrigate = hasAggrigate;
	}

	public String getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	public String getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public boolean isAllColumns() {
		return allColumns;
	}

	public void setAllColumns(boolean allColumns) {
		this.allColumns = allColumns;
	}

	public String getWhereCondition() {
		return whereCondition;
	}

	public void setWhereCondition(String whereCondition) {
		this.whereCondition = whereCondition;
	}

	public List<Restriction> getRestrictionList() {
		return restrictionList;
	}

	public void setRestrictionList(List<Restriction> restrictionList) {
		this.restrictionList = restrictionList;
	}

	public List<String> getLogicalOperators() {
		return logicalOperators;
	}

	public void setLogicalOperators(List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}

	public QueryParser parseQuery(String query) {
		String mainQuery = query;
		if (mainQuery.contains("group by")) {
			groupBy = mainQuery.split("group by")[1].trim();
			mainQuery = mainQuery.split("group by")[0];
			queryType="Group_By_Query";
		}
		if (mainQuery.contains("order by")) {
			orderBy = mainQuery.split("order by")[1].trim();
			mainQuery = mainQuery.split("order by")[0];
			queryType="Order_By_Query";
		}
		if (mainQuery.contains("where")) {
			whereCondition = mainQuery.split("where")[1].trim();
			mainQuery = mainQuery.split("where")[0];
			getRelationalAndLogicalOperators(whereCondition);
			queryType="Where_Query";
		}
		if (mainQuery.contains("from")) {
			filePath = mainQuery.split("from")[1].trim();
			mainQuery = mainQuery.split("from")[0];
		}
		if (mainQuery.contains("select")) {
			String columnSection = mainQuery.split("select")[1].trim();
			computeSelectColumns(columnSection);
		}
		return this;
	}

	private void computeSelectColumns(String columnSection) {
		AggregateFunctions aggregateFunctions;
		if (columnSection.trim().contains("*") && columnSection.trim().length() == 1) {
			fields.add(columnSection.trim());
			this.setAllColumns(true);
		} else {
			String[] columns = columnSection.split(",");
			for (String column : columns) {
				aggregateFunctions = new AggregateFunctions();
				if (column.contains("sum") || column.contains("avg") || column.contains("count")
						|| column.contains("min") || column.contains("max")) {
					String[] function = column.split("\\(");
					String agregate = function[0];
					aggregateFunctions.setFunctionName(agregate);
					String[] columnName = function[1].split("\\)");
					String colName = columnName[0];
					aggregateFunctions.setPropertyName(colName);
					aggregateFunction.add(aggregateFunctions);
					setHasAggrigate(true);

				} else {
					fields.add(column.trim());
				}
			}
		}

	}

	private void getRelationalAndLogicalOperators(String whereCondition) {
		String relationalOperators[] = whereCondition.split("\\s+and\\s+|\\s+or\\s+");

		for (String relationCondition : relationalOperators) {
			Restriction restriction = new Restriction();
			relationCondition = relationCondition.trim();
			String columnAndValue[] = relationCondition.split("([!|=|>|<])+");
			String columnName = columnAndValue[0].trim();
			String columnValue = columnAndValue[1].trim();
			int startIndex = relationCondition.indexOf(columnName) + columnName.length();
			int endIndex = relationCondition.indexOf(columnValue);
			String operator = relationCondition.substring(startIndex, endIndex).trim();
			restriction.setPropertyName(columnName);
			restriction.setPropertyValue(columnValue);
			restriction.setConditionalOperator(operator);
			restrictionList.add(restriction);
		}
		if (relationalOperators.length > 1)
			logicalOperatorList(whereCondition);
	}

	private void logicalOperatorList(String whereCondition) {
		String checkForLogicalOperators[] = whereCondition.split("\\s+");
		for (String logicalOperator : checkForLogicalOperators) {
			if (logicalOperator.trim().equals("and") || logicalOperator.trim().equals("or")) {
				logicalOperators.add(logicalOperator);
			}
		}

	}

}
