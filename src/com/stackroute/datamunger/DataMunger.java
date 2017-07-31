package com.stackroute.datamunger;
import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.result.ResultSet;

public class DataMunger {

	public static void main(String[] args) {

		Query query = new Query();
		ResultSet resultSet= query.executeQuery("select sum(Salary) from c:/Scala/emp.csv group by Name");
		resultSet.forEach((k,v)->System.out.println(v));
        
	}
}
