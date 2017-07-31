package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.sun.org.apache.xpath.internal.functions.Function;

public class CSVFileReader {

	BufferedReader bufferedReader;
	String fileName;

	public CSVFileReader(String fileName) {
		this.fileName = fileName;
	}

	public List<String> getHeader() {
		List<String> header = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(fileName));
			String line = bufferedReader.readLine();
			header = Arrays.asList(line.split(","));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return header;
	}

	public List<List<String>> getData(List<String> fields) {

		List<List<String>> dataSet = new ArrayList<>();
		List<Integer> fieldsIndex=getColumnIndex(fields);
		try {
			bufferedReader = new BufferedReader(new FileReader(fileName));
			if(fields.size()==1 && fields.get(0).equals("*"))
			{
			dataSet = bufferedReader.lines().skip(1).map(line -> Arrays.asList(line.split(",")))
					.collect(Collectors.toList());
			}
			else
			{
				List<String> lines=bufferedReader.lines().collect(Collectors.toList());
				 dataSet=lines.stream()
					     .skip(1)
					     .map(line -> Arrays.asList(line.split(",")))	
					     .map(list -> {
					    	 List<String> data= new ArrayList<>();
					    	 for(int index : fieldsIndex)data.add(list.get(index));
					    	 return data;
					     })
					     .collect(Collectors.toList());	
			}
		} catch (Exception e) {

			e.printStackTrace();
		}

		return dataSet;
	}

	public List<Integer> getColumnIndex(List<String> fields) {
		List<String> header = getHeader();
		List<Integer> selectedIndex = new ArrayList<>();
		for (String headerCol : header) {
			for (String selectedColumns : fields) {
				if (headerCol.equals(selectedColumns)) {
					selectedIndex.add(header.indexOf(headerCol));
				}
			}
		}
		return selectedIndex;
	}
}
