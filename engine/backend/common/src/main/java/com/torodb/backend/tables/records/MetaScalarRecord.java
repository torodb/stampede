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

import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;
import org.jooq.Field;
import org.jooq.Record4;
import org.jooq.Record5;
import org.jooq.Row5;
import org.jooq.impl.UpdatableRecordImpl;

@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public abstract class MetaScalarRecord<TableRefTypeT>
    extends UpdatableRecordImpl<MetaScalarRecord<TableRefTypeT>>
    implements Record5<String, String, TableRefTypeT, FieldType, String> {

  private static final long serialVersionUID = -1107968478;

  /**
   * Setter for <code>torodb.scalar.database</code>.
   */
  public void setDatabase(String value) {
    set(0, value);
  }

  /**
   * Getter for <code>torodb.scalar.database</code>.
   */
  public String getDatabase() {
    return (String) getValue(0);
  }

  /**
   * Setter for <code>torodb.scalar.collection</code>.
   */
  public void setCollection(String value) {
    set(1, value);
  }

  /**
   * Getter for <code>torodb.scalar.collection</code>.
   */
  public String getCollection() {
    return (String) getValue(1);
  }

  /**
   * Setter for <code>torodb.scalar.tableRef</code>.
   */
  public void setTableRef(TableRefTypeT value) {
    set(2, value);
  }

  /**
   * Getter for <code>torodb.scalar.tableRef</code>.
   */
  @SuppressWarnings("unchecked")
  public TableRefTypeT getTableRef() {
    return (TableRefTypeT) getValue(2);
  }

  /**
   * Setter for <code>torodb.scalar.type</code>.
   */
  public void setType(FieldType value) {
    set(3, value);
  }

  /**
   * Getter for <code>torodb.scalar.type</code>.
   */
  public FieldType getType() {
    return (FieldType) getValue(3);
  }

  /**
   * Setter for <code>torodb.scalar.idenftifier</code>.
   */
  public void setIdentifier(String value) {
    set(4, value);
  }

  /**
   * Getter for <code>torodb.scalar.idenftifier</code>.
   */
  public String getIdentifier() {
    return (String) getValue(4);
  }

  // -------------------------------------------------------------------------
  // Primary key information
  // -------------------------------------------------------------------------
  @SuppressWarnings("unchecked")
  /**
   * {@inheritDoc}
   */
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
  public Row5<String, String, TableRefTypeT, FieldType, String> fieldsRow() {
    return (Row5<String, String, TableRefTypeT, FieldType, String>) super.fieldsRow();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row5<String, String, TableRefTypeT, FieldType, String> valuesRow() {
    return (Row5<String, String, TableRefTypeT, FieldType, String>) super.valuesRow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field1() {
    return metaScalarTable.DATABASE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field2() {
    return metaScalarTable.COLLECTION;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<TableRefTypeT> field3() {
    return metaScalarTable.TABLE_REF;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<FieldType> field4() {
    return metaScalarTable.TYPE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field5() {
    return metaScalarTable.IDENTIFIER;
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
  public FieldType value4() {
    return getType();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value5() {
    return getIdentifier();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaScalarRecord<TableRefTypeT> value1(String value) {
    setDatabase(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaScalarRecord<TableRefTypeT> value2(String value) {
    setCollection(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaScalarRecord<TableRefTypeT> value3(TableRefTypeT value) {
    setTableRef(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaScalarRecord<TableRefTypeT> value4(FieldType value) {
    setType(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaScalarRecord<TableRefTypeT> value5(String value) {
    setIdentifier(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaScalarRecord<TableRefTypeT> values(String database, String collection,
      TableRefTypeT tableRef, FieldType type, String identifier);

  public MetaScalarRecord<TableRefTypeT> values(String database, String collection, 
      TableRef tableRef, FieldType type, String identifier) {
    return values(database, collection, toTableRefType(tableRef), type, identifier);
  }

  protected abstract TableRefTypeT toTableRefType(TableRef tableRef);

  public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------
  private final MetaScalarTable<TableRefTypeT, MetaScalarRecord<TableRefTypeT>> metaScalarTable;

  /**
   * Create a detached MetaScalarRecord
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public MetaScalarRecord(MetaScalarTable metaScalarTable) {
    super(metaScalarTable);

    this.metaScalarTable = metaScalarTable;
  }
}
