package com.stackroute.datamunger.restrictions;

public class Restriction {

	private String propertyName;
	private String propertyValue;
	private String conditionalOperator;
	public String getPropertyName() {
		return propertyName;
	}
	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}
	public String getPropertyValue() {
		return propertyValue;
	}
	public void setPropertyValue(String propertyValue) {
		this.propertyValue = propertyValue;
	}
	public String getConditionalOperator() {
		return conditionalOperator;
	}
	public void setConditionalOperator(String conditionalOperator) {
		this.conditionalOperator = conditionalOperator;
	}
	
}
