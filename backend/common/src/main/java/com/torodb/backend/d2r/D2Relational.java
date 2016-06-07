package com.torodb.backend.d2r;

import com.torodb.backend.d2r.model.PathStack;
import com.torodb.backend.d2r.model.PathStack.PathArrayIdx;
import com.torodb.backend.d2r.model.PathStack.PathInfo;
import com.torodb.backend.d2r.model.PathStack.PathNodeType;
import com.torodb.backend.d2r.model.RowInfo;
import com.torodb.backend.d2r.model.TableInfo;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVValue;

public class D2Relational {

	private static final DocumentVisitor visitor = new DocumentVisitor();
	
	private final ConsumerFromArrayIdx fromArrayIdx = new ConsumerFromArrayIdx();
	private final DocConsumer docComsumer = new DocConsumer();
	private final PathStack pathStack = new PathStack();
	private final TableRepository tableRepository;

	public D2Relational(TableRepository tableRepository) {
		this.tableRepository = tableRepository;
	}

	public void translate(KVDocument document) {
		docComsumer.consume(document);
	}

	public class DocConsumer {
		
		public void consume(KVDocument value) {
			PathInfo parentPath = pathStack.peek();
			TableInfo table = tableRepository.findTable(parentPath);
			RowInfo rowInfo = table.newRowObject(getDocumentIndex(parentPath), parentPath.findParentRowInfo()); 
			pathStack.pushObject(rowInfo);
			for (DocEntry<?> entry : value) {
				String key = entry.getKey();
				KVValue<?> entryValue = entry.getValue();
				if (isScalar(entryValue.getType())) {
					rowInfo.addScalar(key, entryValue);
				} else {
					rowInfo.addChild(key, entryValue);
					pathStack.pushField(key);
					entryValue.accept(visitor, docComsumer);
					pathStack.pop();
				}
			}
			pathStack.pop();
		}
		
		public void consume(KVArray value) {
			int i = 0;
			pathStack.pushArray();
			PathInfo current = pathStack.peek();
			TableInfo table = tableRepository.findTable(current);
			for (KVValue<?> val : value) {
				if (isScalar(val.getType())) {
					RowInfo rowInfo = table.newRowObject(i++, current.findParentRowInfo());
					rowInfo.addArrayItem(val);
				}else{
					pathStack.pushArrayIdx(i++);
					val.accept(visitor, fromArrayIdx);
					pathStack.pop();
				}
			}
			pathStack.pop();
		}

	}

	private class ConsumerFromArrayIdx extends DocConsumer {

		@Override
		public void consume(KVArray value) {
			PathArrayIdx current = (PathArrayIdx) pathStack.pop();
			TableInfo table = tableRepository.findTable(current);
			RowInfo rowInfo = table.newRowObject(current.getIdx(), pathStack.peek().findParentRowInfo());
			rowInfo.addChildToArray(value);
			pathStack.pushArrayIdx(current.getIdx(), rowInfo);
			super.consume(value);
		}

	}

	private boolean isScalar(KVType kvType) {
		return (kvType != DocumentType.INSTANCE) && !(kvType instanceof ArrayType);
	}

	private Integer getDocumentIndex(PathInfo path) {
		if (path.is(PathNodeType.Idx)) {
			return ((PathArrayIdx) path).getIdx();
		}
		return null;
	}

}
