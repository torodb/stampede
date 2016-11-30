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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.d2r.CollectionMetaInfo;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class TableMetadata {

  private final CollectionMetaInfo collectionMetaInfo;
  private final TableRef tableRef;
  private final MutableMetaDocPart metaDocPart;

  private final Table<String, FieldType, Integer> fieldOrder;
  private final Map<FieldType, Integer> scalarOrder;

  private final List<MetaField> orderedFields;
  private final List<MetaScalar> orderedScalars;

  public TableMetadata(CollectionMetaInfo collectionMetaInfo, TableRef tableRef) {
    this.collectionMetaInfo = collectionMetaInfo;
    this.tableRef = tableRef;
    this.metaDocPart = collectionMetaInfo.findMetaDocPart(tableRef);
    this.fieldOrder = HashBasedTable.create();
    this.scalarOrder = new EnumMap<>(FieldType.class);
    this.orderedFields = new ArrayList<>(64);
    this.orderedScalars = new ArrayList<>(64);
  }

  protected MutableMetaDocPart getMetaDocPart() {
    return metaDocPart;
  }

  protected List<MetaField> getOrdererdFields() {
    return orderedFields;
  }

  protected List<MetaScalar> getOrdererdScalars() {
    return orderedScalars;
  }

  protected int getNextRowId() {
    return collectionMetaInfo.getNextRowId(tableRef);
  }

  protected int findFieldPosition(String key, FieldType type) {
    Integer idx = fieldOrder.get(key, type);
    if (idx == null) {
      idx = orderedFields.size();
      fieldOrder.put(key, type, idx);
      orderedFields.add(findMetaField(key, type));
    }
    return idx;
  }

  protected int findScalarPosition(FieldType type) {
    Integer idx = scalarOrder.get(type);
    if (idx == null) {
      idx = orderedScalars.size();
      scalarOrder.put(type, idx);
      orderedScalars.add(findMetaScalar(type));
    }
    return idx;
  }

  private MetaField findMetaField(String key, FieldType type) {
    MetaField metaField = metaDocPart.getMetaFieldByNameAndType(key, type);
    if (metaField == null) {
      String identifier = collectionMetaInfo.getFieldIdentifier(tableRef, type, key);
      metaField = metaDocPart.addMetaField(key, identifier, type);
    }
    return metaField;
  }

  private MetaScalar findMetaScalar(FieldType type) {
    MetaScalar metaField = metaDocPart.getScalar(type);
    if (metaField == null) {
      String identifier = collectionMetaInfo.getScalarIdentifier(type);
      metaField = metaDocPart.addMetaScalar(identifier, type);
    }
    return metaField;
  }

}
