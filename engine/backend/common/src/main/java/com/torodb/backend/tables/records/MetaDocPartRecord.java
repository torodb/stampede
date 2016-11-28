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

import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Record5;
import org.jooq.Row5;
import org.jooq.impl.UpdatableRecordImpl;

@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public abstract class MetaDocPartRecord<TableRefTypeT>
    extends UpdatableRecordImpl<MetaDocPartRecord<TableRefTypeT>>
    implements Record5<String, String, TableRefTypeT, String, Integer> {

  private static final long serialVersionUID = -2107968478;

  /**
   * Setter for <code>torodb.container.database</code>.
   */
  public void setDatabase(String value) {
    set(0, value);
  }

  /**
   * Getter for <code>torodb.container.database</code>.
   */
  public String getDatabase() {
    return (String) getValue(0);
  }

  /**
   * Setter for <code>torodb.container.collection</code>.
   */
  public void setCollection(String value) {
    set(1, value);
  }

  /**
   * Getter for <code>torodb.container.collection</code>.
   */
  public String getCollection() {
    return (String) getValue(1);
  }

  /**
   * Setter for <code>torodb.container.tableRef</code>.
   */
  public void setTableRef(TableRefTypeT value) {
    set(2, value);
  }

  /**
   * Getter for <code>torodb.container.tableRef</code>.
   */
  @SuppressWarnings("unchecked")
  public TableRefTypeT getTableRef() {
    return (TableRefTypeT) getValue(2);
  }

  /**
   * Setter for <code>torodb.container.identifier</code>.
   */
  public void setIdentifier(String value) {
    set(3, value);
  }

  /**
   * Getter for <code>torodb.container.identifier</code>.
   */
  public String getIdentifier() {
    return (String) getValue(3);
  }

  /**
   * Setter for <code>torodb.container.last_rid</code>.
   */
  public void setLastRid(Integer value) {
    set(4, value);
  }

  /**
   * Getter for <code>torodb.container.last_rid</code>.
   */
  public Integer getLastRid() {
    return (Integer) getValue(4);
  }

  // -------------------------------------------------------------------------
  // Primary key information
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Record3<String, String, String> key() {
    return (Record3<String, String, String>) super.key();
  }

  // -------------------------------------------------------------------------
  // Record7 type implementation
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row5<String, String, TableRefTypeT, String, Integer> fieldsRow() {
    return (Row5<String, String, TableRefTypeT, String, Integer>) super.fieldsRow();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row5<String, String, TableRefTypeT, String, Integer> valuesRow() {
    return (Row5<String, String, TableRefTypeT, String, Integer>) super.valuesRow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field1() {
    return metaDocPartTable.DATABASE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field2() {
    return metaDocPartTable.COLLECTION;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<TableRefTypeT> field3() {
    return metaDocPartTable.TABLE_REF;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field4() {
    return metaDocPartTable.IDENTIFIER;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<Integer> field5() {
    return metaDocPartTable.LAST_RID;
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
    return getIdentifier();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer value5() {
    return getLastRid();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartRecord<TableRefTypeT> value1(String value) {
    setDatabase(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartRecord<TableRefTypeT> value2(String value) {
    setCollection(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartRecord<TableRefTypeT> value3(TableRefTypeT value) {
    setTableRef(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartRecord<TableRefTypeT> value4(String value) {
    setIdentifier(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartRecord<TableRefTypeT> value5(Integer value) {
    setLastRid(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaDocPartRecord<TableRefTypeT> values(String database, String collection,
      TableRefTypeT tableRef, String identifier, Integer lastRid);

  public MetaDocPartRecord<TableRefTypeT> values(String database, String collection,
      TableRef tableRef, String identifier) {
    return values(database, collection, toTableRefType(tableRef), identifier, 0);
  }

  protected abstract TableRefTypeT toTableRefType(TableRef tableRef);

  public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------
  private final MetaDocPartTable<TableRefTypeT, MetaDocPartRecord<TableRefTypeT>> metaDocPartTable;

  /**
   * Create a detached MetaDocPartRecord
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public MetaDocPartRecord(MetaDocPartTable metaDocPartTable) {
    super(metaDocPartTable);

    this.metaDocPartTable = metaDocPartTable;
  }
}
