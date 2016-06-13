/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.backend;

import com.google.common.collect.ImmutableList;
import com.torodb.backend.AttributeReference.ArrayKey;
import com.torodb.backend.AttributeReference.ObjectKey;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefImpl;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVValue;

public class D2RVisitor<Row> {
    
    private final TableRefFactory tableRefFactory;
    private final Callback<Row> callback;
    private final AttributeReferenceTranslator attrRefTranslator;
    
    public D2RVisitor(TableRefFactory tableRefFactory, AttributeReferenceTranslator attrRefTranslator, Callback<Row> callback) {
        this.tableRefFactory = tableRefFactory;
        this.callback = callback;
		this.attrRefTranslator = attrRefTranslator;
    }
    
    public void visit(KVDocument document) {
        visit(document, new Argument<>(tableRefFactory));
    }
    
    private void visit(KVDocument document, Argument<Row> arg) {
        Row newRow = callback.visit(document, arg.attributeReference, arg.tableRef, arg.parentRow);
        
        for (DocEntry<?> entry : document) {
            AttributeReference attributeReference = arg.attributeReference.append(new ObjectKey(entry.getKey()));
            TableRef tableRef = attrRefTranslator.toTableRef(attributeReference);
            Argument<Row> entryArg = new Argument<Row>(attributeReference, tableRef, newRow);
            
            KVValue<?> value = entry.getValue();
            if (value instanceof KVArray) {
                visit((KVArray) value, entryArg);
            } else if (value instanceof KVDocument) {
                visit((KVDocument) value, entryArg);
            }
        }
    }
    
    private void visit(KVArray array, Argument<Row> arg) {
        callback.visit(array, arg.attributeReference, arg.tableRef, arg.parentRow);
        AttributeReference attributeReference = arg.attributeReference.append(new ArrayKey(0));
        TableRef tableRef = attrRefTranslator.toTableRef(attributeReference);
        int index = 0;
        for (KVValue<?> element : array) {
            attributeReference = arg.attributeReference.append(new ArrayKey(index));
            Argument<Row> elementArg = new Argument<Row>(attributeReference, tableRef, arg.parentRow);
            
            if (element instanceof KVArray) {
                Row newRow = callback.visitArrayElement((KVArray) element, attributeReference, tableRef, index, arg.parentRow);
                Argument<Row> elementArrayArg = new Argument<Row>(attributeReference, tableRef, newRow);
                visit((KVArray) element, elementArrayArg);
            } else if (element instanceof KVDocument) {
                visit((KVDocument) element, elementArg);
            }
            
            index++;
        }
    }

    public interface Callback<Row> {
        public Row visit(KVDocument document, AttributeReference attributeReference, TableRef tableRef, Row parentRow);
        public void visit(KVArray array, AttributeReference attributeReference, TableRef tableRef, Row parentRow);
        public Row visitArrayElement(KVArray array, AttributeReference attributeReference, TableRef tableRef, int index, Row parentRow);
    }
    
    public static class Argument<Row> {
        public final AttributeReference attributeReference;
        public final TableRef tableRef;
        public final Row parentRow;
        
        public Argument(TableRefFactory tableRefFactory) {
            super();
            this.attributeReference = new AttributeReference(ImmutableList.of());
            this.tableRef = tableRefFactory.createRoot();
            this.parentRow = null;
        }
        
        public Argument(AttributeReference attributeReference, TableRef tableRef, Row parentRow) {
            super();
            this.attributeReference = attributeReference;
            this.tableRef = tableRef;
            this.parentRow = parentRow;
        }
    }
}
