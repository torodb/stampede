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

package com.torodb.backend.tables.records;

import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;
import org.jooq.Field;
import org.jooq.Record4;
import org.jooq.Record6;
import org.jooq.Row6;
import org.jooq.impl.UpdatableRecordImpl;

@SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:MemberName",
    "checkstyle:OverloadMethodsDeclarationOrder"})
public abstract class MetaFieldRecord<TableRefTypeT>
    extends UpdatableRecordImpl<MetaFieldRecord<TableRefTypeT>>
    implements Record6<String, String, TableRefTypeT, String, FieldType, String> {

  private static final long serialVersionUID = -2107968478;

  /**
   * Setter for <code>torodb.field.database</code>.
   */
  public void setDatabase(String value) {
    set(0, value);
  }

  /**
   * Getter for <code>torodb.field.database</code>.
   */
  public String getDatabase() {
    return (String) getValue(0);
  }

  /**
   * Setter for <code>torodb.field.collection</code>.
   */
  public void setCollection(String value) {
    set(1, value);
  }

  /**
   * Getter for <code>torodb.field.collection</code>.
   */
  public String getCollection() {
    return (String) getValue(1);
  }

  /**
   * Setter for <code>torodb.field.tableRef</code>.
   */
  public void setTableRef(TableRefTypeT value) {
    set(2, value);
  }

  /**
   * Getter for <code>torodb.field.tableRef</code>.
   */
  @SuppressWarnings("unchecked")
  public TableRefTypeT getTableRef() {
    return (TableRefTypeT) getValue(2);
  }

  /**
   * Setter for <code>torodb.field.name</code>.
   */
  public void setName(String value) {
    set(3, value);
  }

  /**
   * Getter for <code>torodb.field.name</code>.
   */
  public String getName() {
    return (String) getValue(3);
  }

  /**
   * Setter for <code>torodb.field.type</code>.
   */
  public void setType(FieldType value) {
    set(4, value);
  }

  /**
   * Getter for <code>torodb.field.type</code>.
   */
  public FieldType getType() {
    return (FieldType) getValue(4);
  }

  /**
   * Setter for <code>torodb.field.idenftifier</code>.
   */
  public void setIdentifier(String value) {
    set(5, value);
  }

  /**
   * Getter for <code>torodb.field.idenftifier</code>.
   */
  public String getIdentifier() {
    return (String) getValue(5);
  }

  // -------------------------------------------------------------------------
  // Primary key information
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Record4<String, String, String, String> key() {
    return (Record4<String, String, String, String>) super.key();
  }

  // -------------------------------------------------------------------------
  // Record7 type implementation
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row6<String, String, TableRefTypeT, String, FieldType, String> fieldsRow() {
    return (Row6<String, String, TableRefTypeT, String, FieldType, String>) super.fieldsRow();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row6<String, String, TableRefTypeT, String, FieldType, String> valuesRow() {
    return (Row6<String, String, TableRefTypeT, String, FieldType, String>) super.valuesRow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field1() {
    return metaFieldTable.DATABASE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field2() {
    return metaFieldTable.COLLECTION;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<TableRefTypeT> field3() {
    return metaFieldTable.TABLE_REF;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field4() {
    return metaFieldTable.NAME;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<FieldType> field5() {
    return metaFieldTable.TYPE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field6() {
    return metaFieldTable.IDENTIFIER;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value1() {
    return getDatabase();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value2() {
    return getCollection();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TableRefTypeT value3() {
    return getTableRef();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value4() {
    return getName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FieldType value5() {
    return getType();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value6() {
    return getIdentifier();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaFieldRecord<TableRefTypeT> value1(String value) {
    setDatabase(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaFieldRecord<TableRefTypeT> value2(String value) {
    setCollection(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaFieldRecord<TableRefTypeT> value3(TableRefTypeT value) {
    setTableRef(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaFieldRecord<TableRefTypeT> value4(String value) {
    setName(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaFieldRecord<TableRefTypeT> value5(FieldType value) {
    setType(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaFieldRecord<TableRefTypeT> value6(String value) {
    setIdentifier(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaFieldRecord<TableRefTypeT> values(String database, String collection,
      TableRefTypeT tableRef, String name, FieldType type, String identifier);

  public MetaFieldRecord<TableRefTypeT> values(String database, String collection,
      TableRef tableRef, String name, FieldType type, String identifier) {
    return values(database, collection, toTableRefType(tableRef), name, type, identifier);
  }

  protected abstract TableRefTypeT toTableRefType(TableRef tableRef);

  public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------
  private final MetaFieldTable<TableRefTypeT, MetaFieldRecord<TableRefTypeT>> metaFieldTable;

  /**
   * Create a detached MetaFieldRecord
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public MetaFieldRecord(MetaFieldTable metaFieldTable) {
    super(metaFieldTable);

    this.metaFieldTable = metaFieldTable;
  }
}
