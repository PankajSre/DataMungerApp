package com.stackroute.datamunger.restrictions;

import java.util.Comparator;
import java.util.List;



public class OrderBySort implements Comparator<List<String>>
{

	private int sortingIndex;
	
	public int getSortingIndex() {
		return sortingIndex;
	}

	public void setSortingIndex(int sortingIndex) {
		this.sortingIndex = sortingIndex;
		
	}

	@Override
	public int compare(List<String> resultSet1,List<String> resultSet2) {
		
		return resultSet1.get(sortingIndex).compareTo(resultSet2.get(sortingIndex));
	}




}