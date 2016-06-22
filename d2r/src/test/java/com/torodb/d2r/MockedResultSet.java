package com.torodb.d2r;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MockedResultSet {

	private Iterator<MockedRow> iterator;
	private MockedRow current = null;

	public MockedResultSet(MockedRow... rows) {
		List<MockedRow> rowsList = Arrays.asList(rows);
		iterator = rowsList.iterator();
	}
	
	public MockedResultSet(List<MockedRow> rows) {
		List<MockedRow> rowsList = new ArrayList<>(rows);
		iterator = rowsList.iterator();
	}

	public boolean next() {
		boolean hasNext = iterator.hasNext();
		if (hasNext) {
			current = iterator.next();
		} else {
			current = null;
		}
		return hasNext;
	}

	public MockedRow getCurrent(){
		return current;
	}
	
	public static class MockedRow {
		private List<Object> values;

		private Integer did = null;
		private Integer pid = null;
		private Integer rid = null;
		private Integer seq = null;

		public MockedRow(Integer did, Integer pid, Integer rid, Integer seq, Object... values) {
			this.values = Arrays.asList(values);
			this.did = did;
			this.pid = pid;
			this.rid = rid;
			this.seq = seq;
		}

		public Integer getDid() {
			return did;
		}

		public Integer getPid() {
			return pid;
		}

		public Integer getRid() {
			return rid;
		}

		public Integer getSeq() {
			return seq;
		}

		public Object get(int index){
			return values.get(index);
		}
	}
	
}
