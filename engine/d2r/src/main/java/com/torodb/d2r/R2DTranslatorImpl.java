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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.DocPartResultRow;
import com.torodb.core.d2r.InternalFields;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.ListKvArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class R2DTranslatorImpl implements R2DTranslator {

  @Override
  public List<ToroDocument> translate(Iterator<DocPartResult> docPartResultIt) {
    ImmutableList.Builder<ToroDocument> readedDocuments = ImmutableList.builder();

    Table<TableRef, Integer, Map<String, List<KvValue<?>>>> currentFieldDocPartTable =
        HashBasedTable.<TableRef, Integer, Map<String, List<KvValue<?>>>>create();
    Table<TableRef, Integer, Map<String, List<KvValue<?>>>> childFieldDocPartTable =
        HashBasedTable.<TableRef, Integer, Map<String, List<KvValue<?>>>>create();

    int previousDepth = -1;

    while (docPartResultIt.hasNext()) {
      DocPartResult docPartResult = docPartResultIt.next();
      MetaDocPart metaDocPart = docPartResult.getMetaDocPart();
      TableRef tableRef = metaDocPart.getTableRef();

      if (previousDepth != -1 && previousDepth != tableRef.getDepth()) {
        Table<TableRef, Integer, Map<String, List<KvValue<?>>>> previousFieldChildDocPartTable =
            childFieldDocPartTable;
        childFieldDocPartTable = currentFieldDocPartTable;
        currentFieldDocPartTable = previousFieldChildDocPartTable;

        if (!tableRef.isRoot()) {
          currentFieldDocPartTable.clear();
        }
      }
      previousDepth = tableRef.getDepth();

      Map<Integer, Map<String, List<KvValue<?>>>> childFieldDocPartRow =
          childFieldDocPartTable.row(tableRef);
      Map<Integer, Map<String, List<KvValue<?>>>> currentFieldDocPartRow;

      if (tableRef.isRoot()) {
        currentFieldDocPartRow = null;
      } else {
        currentFieldDocPartRow = currentFieldDocPartTable.row(tableRef.getParent().get());
      }

      readResult(metaDocPart, tableRef, docPartResult,
          currentFieldDocPartRow, childFieldDocPartRow, readedDocuments);
    }

    return readedDocuments.build();
  }

  private void readResult(MetaDocPart metaDocPart, TableRef tableRef, DocPartResult docPartResult,
      Map<Integer, Map<String, List<KvValue<?>>>> currentFieldDocPartRow,
      Map<Integer, Map<String, List<KvValue<?>>>> childFieldDocPartRow,
      ImmutableList.Builder<ToroDocument> readedDocuments) {
    while (docPartResult.hasNext()) {
      KvDocument.Builder documentBuilder = new KvDocument.Builder();

      DocPartResultRow row = docPartResult.next();

      Integer did = row.getDid();
      Integer rid = row.getRid();
      Integer pid = row.getPid();
      Integer seq = row.getSeq();

      Map<String, List<KvValue<?>>> childFieldDocPartCell = childFieldDocPartRow.get(rid);
      //TODO: ensure MetaField order using ResultSet meta data
      Iterator<? extends MetaScalar> metaScalarIterator = metaDocPart
          .streamScalars().iterator();

      boolean wasScalar = false;
      int fieldIndex = 0;
      while (metaScalarIterator.hasNext() && !wasScalar) {
        assert seq != null : "found scalar value outside of an array";

        MetaScalar metaScalar = metaScalarIterator.next();
        KvValue<?> value = row.getUserValue(fieldIndex, metaScalar.getType());
        fieldIndex++;

        if (value != null) {
          if (metaScalar.getType() == FieldType.CHILD) {
            value = getChildValue(value, getDocPartCellName(tableRef), childFieldDocPartCell);
          }
          addValueToDocPartRow(currentFieldDocPartRow, tableRef, pid, seq, value);
          wasScalar = true;
        }
      }

      if (wasScalar) {
        continue;
      }

      Iterator<? extends MetaField> metaFieldIterator = metaDocPart
          .streamFields().iterator();
      while (metaFieldIterator.hasNext()) {
        MetaField metaField = metaFieldIterator.next();
        KvValue<?> value = row.getUserValue(fieldIndex, metaField.getType());
        fieldIndex++;
        if (value != null) {
          if (metaField.getType() == FieldType.CHILD) {
            value = getChildValue(value, metaField.getName(), childFieldDocPartCell);
          }
          documentBuilder.putValue(metaField.getName(), value);
        }
      }

      if (tableRef.isRoot()) {
        readedDocuments.add(new ToroDocument(did, documentBuilder.build()));
      } else {
        addValueToDocPartRow(currentFieldDocPartRow, tableRef, pid, seq, documentBuilder.build());
      }
    }
  }

  private KvValue<?> getChildValue(KvValue<?> value, String key,
      Map<String, List<KvValue<?>>> childDocPartCell) {
    KvBoolean child = (KvBoolean) value;
    if (child.getValue() == InternalFields.CHILD_ARRAY_VALUE) {
      List<KvValue<?>> elements;
      if (childDocPartCell == null || (elements = childDocPartCell.get(key)) == null) {
        value = new ListKvArray(ImmutableList.of());
      } else {
        value = new ListKvArray(elements);
      }
    } else {
      value = childDocPartCell.get(key).get(0);
    }
    return value;
  }

  private void addValueToDocPartRow(Map<Integer, Map<String, List<KvValue<?>>>> currentDocPartRow,
      TableRef tableRef,
      Integer pid, Integer seq, KvValue<?> value) {
    if (seq == null) {
      setDocPartRowValue(currentDocPartRow, tableRef, pid, null, ImmutableList.of(value));
    } else {
      addToDocPartRow(currentDocPartRow, tableRef, pid, seq, value);
    }
  }

  private void setDocPartRowValue(
      Map<Integer, Map<String, List<KvValue<?>>>> docPartRow, TableRef tableRef, Integer pid,
      Integer seq, ImmutableList<KvValue<?>> elements) {
    Map<String, List<KvValue<?>>> docPartCell = getDocPartCell(docPartRow, pid);
    String name = getDocPartCellName(tableRef);
    docPartCell.put(name, elements);
  }

  private void addToDocPartRow(
      Map<Integer, Map<String, List<KvValue<?>>>> docPartRow, TableRef tableRef, Integer pid,
      Integer seq, KvValue<?> value) {
    String name = getDocPartCellName(tableRef);

    Map<String, List<KvValue<?>>> docPartCell = getDocPartCell(docPartRow, pid);

    List<KvValue<?>> elements = docPartCell
        .computeIfAbsent(name, n -> new ArrayList<KvValue<?>>());
    final int size = elements.size();
    if (seq < size) {
      elements.set(seq, value);
    } else {
      for (int i = elements.size(); i < seq; i++) {
        elements.add(null);
      }
      elements.add(value);
    }
  }

  private String getDocPartCellName(TableRef tableRef) {
    while (tableRef.isInArray()) {
      tableRef = tableRef.getParent().get();
    }
    return tableRef.getName();
  }

  private Map<String, List<KvValue<?>>> getDocPartCell(
      Map<Integer, Map<String, List<KvValue<?>>>> docPartRow,
      Integer pid) {
    return docPartRow.computeIfAbsent(pid, p -> new HashMap<String, List<KvValue<?>>>());
  }
}
