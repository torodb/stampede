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

package com.torodb.d2r.model;

import com.google.common.collect.Iterators;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.d2r.InternalFields;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DocPartRowImpl implements DocPartRow {

  private final int did;
  private final int rid;
  private final Integer pid;
  private final Integer seq;

  private final ArrayList<KvValue<?>> fieldAttributes;
  private final ArrayList<KvValue<?>> scalarAttributes;
  private final TableMetadata tableMetadata;
  private final DocPartDataImpl tableInfo;

  public DocPartRowImpl(TableMetadata tableMetadata, Integer seq, DocPartRowImpl parentRow,
      DocPartDataImpl tableInfo) {
    this.tableInfo = tableInfo;
    this.rid = tableMetadata.getNextRowId();
    this.seq = seq;
    if (parentRow == null) {
      this.did = this.rid;
      this.pid = null;
    } else {
      this.did = parentRow.did;
      this.pid = parentRow.rid;
    }
    this.tableMetadata = tableMetadata;
    this.fieldAttributes = new ArrayList<KvValue<?>>(); //initialize with metadata current size?
    this.scalarAttributes = new ArrayList<KvValue<?>>();
  }

  private static final KvBoolean IS_ARRAY = KvBoolean.from(InternalFields.CHILD_ARRAY_VALUE);
  private static final KvBoolean IS_SUBDOCUMENT = KvBoolean.from(InternalFields.CHILD_OBJECT_VALUE);

  public void addScalar(String key, KvValue<?> value) {
    final int position = findFieldPosition(key, FieldType.from(value.getType()));
    fieldAttributes.set(position, value);
  }

  public void addChild(String key, KvValue<?> value) {
    final int position = findFieldPosition(key, FieldType.from(value.getType()));
    if (value instanceof KvArray) {
      fieldAttributes.set(position, IS_ARRAY);
    } else if (value instanceof KvDocument) {
      fieldAttributes.set(position, IS_SUBDOCUMENT);
    } else {
      throw new IllegalArgumentException("Child value is not KVArray or KVDocument");
    }
  }

  public void addArrayItem(KvValue<?> value) {
    Integer position = findScalarPosition(FieldType.from(value.getType()));
    scalarAttributes.set(position, value);
  }

  public void addChildToArray(KvValue<?> value) {
    final int position = findScalarPosition(FieldType.from(value.getType()));
    if (value instanceof KvArray) {
      scalarAttributes.set(position, IS_ARRAY);
    } else if (value instanceof KvDocument) {
      scalarAttributes.set(position, IS_SUBDOCUMENT);
    } else {
      throw new IllegalArgumentException("Child value is not KVArray or KVDocument");
    }
  }

  private int findFieldPosition(String key, FieldType fieldType) {
    final int position = tableMetadata.findFieldPosition(key, fieldType);
    if (position >= fieldAttributes.size()) {
      for (int index = fieldAttributes.size(); index <= position; index++) {
        fieldAttributes.add(null);
      }
    }
    return position;
  }

  private int findScalarPosition(FieldType fieldType) {
    final int position = tableMetadata.findScalarPosition(fieldType);
    if (position >= scalarAttributes.size()) {
      for (int index = scalarAttributes.size(); index <= position; index++) {
        scalarAttributes.add(null);
      }
    }
    return position;
  }

  @Override
  public DocPartData getDocPartData() {
    return tableInfo;
  }

  @Override
  public Integer getSeq() {
    return seq;
  }

  @Override
  public int getDid() {
    return did;
  }

  @Override
  public int getRid() {
    return rid;
  }

  @Override
  public Integer getPid() {
    return pid;
  }

  @Override
  public Iterable<KvValue<?>> getFieldValues() {
    int columns = this.getDocPartData().fieldColumnsCount();
    int attrs = this.fieldAttributes.size();
    if (columns == attrs) {
      return fieldAttributes;
    }
    NumberNullIterator<KvValue<?>> itTail = new NumberNullIterator<>(columns - attrs);
    return () -> Iterators.concat(fieldAttributes.iterator(), itTail);
  }

  @Override
  public Iterable<KvValue<?>> getScalarValues() {
    int columns = this.getDocPartData().scalarColumnsCount();
    int attrs = this.scalarAttributes.size();
    if (columns == attrs) {
      return scalarAttributes;
    }
    NumberNullIterator<KvValue<?>> itTail = new NumberNullIterator<>(columns - attrs);
    return () -> Iterators.concat(scalarAttributes.iterator(), itTail);
  }

  private static class NumberNullIterator<R> implements Iterator<R> {

    private final int n;

    private int idx;

    public NumberNullIterator(int n) {
      this.n = n;
      this.idx = 0;
    }

    @Override
    public boolean hasNext() {
      return idx < n;
    }

    @Override
    public R next() {
      if (hasNext()) {
        idx++;
        return null;
      }
      throw new NoSuchElementException();
    }

  }

}
