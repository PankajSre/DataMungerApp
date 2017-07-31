package com.stackroute.datamunger.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.result.ResultSet;


public class DataMungerTest {

	static Query query;
	static ResultSet resultSet;

	
	@BeforeClass
	public static void init()
	{
		query = new Query();
		resultSet= new ResultSet();
		
	}
	 
	@Test
     public void selectAllWithoutWhereTestCase(){
		String queryString="select * from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
	@Test
	public void selectColumnsWithoutWhereTestCase(){
		String queryString="select City,Salary,Name from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
	@Test
	public void withWhereGreaterThanTestCase(){
		String queryString="select City,Dept,Name from C:/Scala/emp.csv where Salary >30000";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
     
	@Test
	public void withWhereLessThanTestCase(){
		
		String queryString="select City,Dept,Name from C:/Scala/emp.csv where Salary<30000";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
	@Test
	public void withWhereLessThanOrEqualToTestCase(){
		
		String queryString="select City,Dept,Name from C:/Scala/emp.csv where Salary>30000 and Name=Rakesh";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
    
	@Test
	public void withWhereGreaterThanOrEqualToTestCase(){
		
		String queryString="select City,Dept,Name from C:/Scala/emp.csv where Salary>=33000 and Name=Rakesh";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
    
	@Test
	public void withWhereNotEqualToTestCase(){
		
		String queryString="select City,Dept,Name from C:/Scala/emp.csv where Salary!=33000 and Name !=Rakesh";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
	@Test
	public void withWhereEqualAndNotEqualTestCase(){
		

		String queryString="select City,Name from C:/Scala/emp.csv where Salary=35000 and Name !=Rakesh";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
	@Test
	public void testAllColumnsWithoutWhereWithOrderByTestCase(){
		
		String queryString="select * from C:/Scala/emp.csv order by Name";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
    @Test
	public void selectOrderByColumnsWithWhereTestCase(){
		
		String queryString="select EmpId,City,Salary from C:/Scala/emp.csv order by Name";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
     
	@Test
	public void selectOrderByWithoutWhereColumnsWithWhereTestCase(){
		
		String queryString="select EmpId,City,Salary from C:/Scala/emp.csv where Salary>35000 order by Name";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
     
	@Test
	public void selectSumColumnsWithTestCase(){
		
		String queryString="select sum(Salary) from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
     
	@Test
	public void selectCountAllColumnsTestCase(){
		
		String queryString="select count(Salary) from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
	}
     
	@Test
	public void selectAverageSalaryColumnsTestCase(){
		
		String queryString="select avg(Salary) from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	@Test
	 
	public void selectMinSalaryColumnsWithWhereTestCase(){
		
		String queryString="select min(Salary) from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
	@Test
	public void selectMaxSalaryColumnsWithWhereTestCase(){
		String queryString="select max(Salary) from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
	@Test
	public void selectLogicalOperatorAndColumnsWithWhereTestCase(){
		
		String queryString="select min(Salary) from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
	 
	@Test
	public void selectMultipleAggregateTestCase(){
		
		String queryString="select sum(Salary),count(Name) from C:/Scala/emp.csv";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
    
   @Test
	public void selectGroupByColumnsWithWhereTestCase(){
		
	   String queryString="select count(Name) from C:/Scala/emp.csv group by Name";
		resultSet=query.executeQuery(queryString);
		assertNotNull(resultSet);
		display(queryString,resultSet);
		
	}
     @Test
  	public void selectCountWithGroupByColumnsWithWhereTestCase(){
  		
  	   String queryString="select count(City) from C:/Scala/emp.csv group by City";
  		resultSet=query.executeQuery(queryString);
  		assertNotNull(resultSet);
  		display(queryString,resultSet);
  		
  	}
	
	
	private void display(String testCaseName, ResultSet resultSet2) {
		System.out.println(testCaseName);
		System.out.println("******************************************");
		resultSet.forEach((k,v)->System.out.println(v));
		System.out.println("******************************************");
	}
	

}
