package com.spark;

import java.util.List;

public class WriterInfo {
	String keySpace;
	String tableName;
	List<String> columnNames;
	List<Object> columnValues;
	
	public WriterInfo(String keySpace, String tableName, List<String> columnNames, List<Object> columnValues) {
		this.keySpace = keySpace;
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.columnValues = columnValues;
		
	}
	
	public String getKeySpace() {
		return keySpace;
	}
	public void setKeySpace(String keySpace) {
		this.keySpace = keySpace;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public List<String> getColumnNames() {
		return columnNames;
	}
	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}
	public List<Object> getColumnValues() {
		return columnValues;
	}
	public void setColumnValues(List<Object> columnValues) {
		this.columnValues = columnValues;
	}
}
