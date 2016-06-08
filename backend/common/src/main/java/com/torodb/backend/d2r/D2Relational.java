package com.torodb.backend.d2r;

import com.torodb.backend.d2r.model.PathStack;
import com.torodb.backend.d2r.model.PathStack.PathArrayIdx;
import com.torodb.backend.d2r.model.PathStack.PathInfo;
import com.torodb.backend.d2r.model.PathStack.PathNodeType;
import com.torodb.backend.d2r.model.DocPartRowImpl;
import com.torodb.backend.d2r.model.DocPartDataImpl;
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
	private final DocPartDataCollection docPartDataCollection;

	public D2Relational(DocPartDataCollection docPartDataCollection) {
		this.docPartDataCollection = docPartDataCollection;
	}

	public void translate(KVDocument document) {
		docComsumer.consume(document);
	}

	public class DocConsumer {
		
		public void consume(KVDocument value) {
			PathInfo parentPath = pathStack.peek();
			DocPartDataImpl docPartData = docPartDataCollection.findDocPartData(parentPath);
			DocPartRowImpl docPartRow = docPartData.newRowObject(getDocumentIndex(parentPath), parentPath.findParentRowInfo()); 
			pathStack.pushObject(docPartRow);
			for (DocEntry<?> entry : value) {
				String key = entry.getKey();
				KVValue<?> entryValue = entry.getValue();
				if (isScalar(entryValue.getType())) {
					docPartRow.addScalar(key, entryValue);
				} else {
					docPartRow.addChild(key, entryValue);
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
			DocPartDataImpl table = docPartDataCollection.findDocPartData(current);
			for (KVValue<?> val : value) {
				if (isScalar(val.getType())) {
					DocPartRowImpl rowInfo = table.newRowObject(i++, current.findParentRowInfo());
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
			DocPartDataImpl docPartData = docPartDataCollection.findDocPartData(current);
			DocPartRowImpl docPartRow = docPartData.newRowObject(current.getIdx(), pathStack.peek().findParentRowInfo());
			docPartRow.addChildToArray(value);
			pathStack.pushArrayIdx(current.getIdx(), docPartRow);
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
