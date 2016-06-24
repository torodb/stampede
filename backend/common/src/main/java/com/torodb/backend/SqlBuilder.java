package com.torodb.backend;

public class SqlBuilder {

	private final StringBuilder sb;

	public SqlBuilder(String init) {
		this.sb = new StringBuilder();
		this.sb.append(init);
	}

	public SqlBuilder(StringBuilder sb) {
		this.sb = sb;
	}
	
	public SqlBuilder append(String str){
		sb.append(str);
		return this;
	}
	
	public SqlBuilder quote(String str){
		sb.append('"').append(str).append('"');
		return this;
	}
	
	public SqlBuilder quote(Enum<?> enumValue){
		sb.append('"').append(enumValue.toString()).append('"');
		return this;
	}

	
	public SqlBuilder table(String schema, String table){
		sb.append('"').append(schema).append("\".\"").append(table).append('"');
		return this;
	}
	
	public String toString(){
		return sb.toString();
	}
}
