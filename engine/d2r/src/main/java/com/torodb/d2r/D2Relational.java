/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.d2r;

import com.torodb.core.TableRefFactory;
import com.torodb.d2r.model.DocPartDataImpl;
import com.torodb.d2r.model.DocPartRowImpl;
import com.torodb.d2r.model.PathStack;
import com.torodb.d2r.model.PathStack.PathArrayIdx;
import com.torodb.d2r.model.PathStack.PathInfo;
import com.torodb.d2r.model.PathStack.PathNodeType;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvDocument.DocEntry;
import com.torodb.kvdocument.values.KvValue;

public class D2Relational {

  private static final DocumentVisitor visitor = new DocumentVisitor();

  private final ConsumerFromArrayIdx fromArrayIdx = new ConsumerFromArrayIdx();
  private final DocConsumer docComsumer = new DocConsumer();
  private final PathStack pathStack;
  private final DocPartDataCollection docPartDataCollection;

  public D2Relational(TableRefFactory tableRefFactory,
      DocPartDataCollection docPartDataCollection) {
    this.pathStack = new PathStack(tableRefFactory);
    this.docPartDataCollection = docPartDataCollection;
  }

  public void translate(KvDocument document) {
    docComsumer.consume(document);
  }

  public class DocConsumer {

    public void consume(KvDocument value) {
      PathInfo parentPath = pathStack.peek();
      DocPartDataImpl docPartData = docPartDataCollection.findDocPartData(parentPath);
      DocPartRowImpl docPartRow = docPartData.newRowObject(getDocumentIndex(parentPath), parentPath
          .findParentRowInfo());
      pathStack.pushObject(docPartRow);
      for (DocEntry<?> entry : value) {
        String key = entry.getKey();
        KvValue<?> entryValue = entry.getValue();
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

    public void consume(KvArray value) {
      int i = 0;
      pathStack.pushArray();
      PathInfo current = pathStack.peek();
      DocPartDataImpl table = docPartDataCollection.findDocPartData(current);
      for (KvValue<?> val : value) {
        if (isScalar(val.getType())) {
          DocPartRowImpl rowInfo = table.newRowObject(i++, current.findParentRowInfo());
          rowInfo.addArrayItem(val);
        } else {
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
    public void consume(KvArray value) {
      PathArrayIdx current = (PathArrayIdx) pathStack.pop();
      DocPartDataImpl docPartData = docPartDataCollection.findDocPartData(current);
      DocPartRowImpl docPartRow = docPartData.newRowObject(current.getIdx(), pathStack.peek()
          .findParentRowInfo());
      docPartRow.addChildToArray(value);
      pathStack.pushArrayIdx(current.getIdx(), docPartRow);
      super.consume(value);
    }

  }

  private boolean isScalar(KvType kvType) {
    return (kvType != DocumentType.INSTANCE) && !(kvType instanceof ArrayType);
  }

  private Integer getDocumentIndex(PathInfo path) {
    if (path.is(PathNodeType.Idx)) {
      return ((PathArrayIdx) path).getIdx();
    }
    return null;
  }

}
