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

import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Record7;
import org.jooq.Row7;
import org.jooq.impl.UpdatableRecordImpl;

@SuppressWarnings({"checkstyle:LineLength", "checkstyle:AbbreviationAsWordInName",
    "checkstyle:MemberName", "checkstyle:OverloadMethodsDeclarationOrder"})
public abstract class MetaDocPartIndexColumnRecord<TableRefTypeT>
    extends UpdatableRecordImpl<MetaDocPartIndexColumnRecord<TableRefTypeT>>
    implements Record7<String, String, Integer, String, TableRefTypeT, String, FieldIndexOrdering> {

  private static final long serialVersionUID = 4696260394225350907L;

  /**
   * Setter for <code>torodb.doc_part_index_column.database</code>.
   */
  public void setDatabase(String value) {
    set(0, value);
  }

  /**
   * Getter for <code>torodb.doc_part_index_column.database</code>.
   */
  public String getDatabase() {
    return (String) getValue(0);
  }

  /**
   * Setter for <code>torodb.doc_part_index_column.index_identifier</code>.
   */
  public void setIndexIdentifier(String value) {
    set(1, value);
  }

  /**
   * Getter for <code>torodb.doc_part_index_column.index_identifier</code>.
   */
  public String getIndexIdentifier() {
    return (String) getValue(1);
  }

  /**
   * Setter for <code>torodb.doc_part_index_column.position</code>.
   */
  public void setPosition(Integer value) {
    set(2, value);
  }

  /**
   * Getter for <code>torodb.doc_part_index_column.position</code>.
   */
  public Integer getPosition() {
    return (Integer) getValue(2);
  }

  /**
   * Setter for <code>torodb.doc_part_index_column.collection</code>.
   */
  public void setCollection(String value) {
    set(3, value);
  }

  /**
   * Getter for <code>torodb.doc_part_index_column.collection</code>.
   */
  public String getCollection() {
    return (String) getValue(3);
  }

  /**
   * Setter for <code>torodb.doc_part_index_column.tableRef</code>.
   */
  public void setTableRef(TableRefTypeT value) {
    set(4, value);
  }

  /**
   * Getter for <code>torodb.doc_part_index_column.tableRef</code>.
   */
  @SuppressWarnings("unchecked")
  public TableRefTypeT getTableRef() {
    return (TableRefTypeT) getValue(4);
  }

  /**
   * Setter for <code>torodb.doc_part_index_column.identifier</code>.
   */
  public void setIdentifier(String value) {
    set(5, value);
  }

  /**
   * Getter for <code>torodb.doc_part_index_column.identifier</code>.
   */
  public String getIdentifier() {
    return (String) getValue(5);
  }

  /**
   * Setter for <code>torodb.doc_part_index_column.ordering</code>.
   */
  public void setOrdering(FieldIndexOrdering value) {
    set(6, value);
  }

  /**
   * Getter for <code>torodb.doc_part_index_column.ordering</code>.
   */
  public FieldIndexOrdering getOrdering() {
    return (FieldIndexOrdering) getValue(6);
  }

  // -------------------------------------------------------------------------
  // Primary key information
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Record3<String, String, Integer> key() {
    return (Record3<String, String, Integer>) super.key();
  }

  // -------------------------------------------------------------------------
  // Record7 type implementation
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row7<String, String, Integer, String, TableRefTypeT, String, FieldIndexOrdering> fieldsRow() {
    return (Row7<String, String, Integer, String, TableRefTypeT, String, FieldIndexOrdering>) super
        .fieldsRow();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row7<String, String, Integer, String, TableRefTypeT, String, FieldIndexOrdering> valuesRow() {
    return (Row7<String, String, Integer, String, TableRefTypeT, String, FieldIndexOrdering>) super
        .valuesRow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field1() {
    return metaIndexFieldTable.DATABASE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field2() {
    return metaIndexFieldTable.INDEX_IDENTIFIER;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<Integer> field3() {
    return metaIndexFieldTable.POSITION;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field4() {
    return metaIndexFieldTable.COLLECTION;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<TableRefTypeT> field5() {
    return metaIndexFieldTable.TABLE_REF;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field6() {
    return metaIndexFieldTable.IDENTIFIER;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<FieldIndexOrdering> field7() {
    return metaIndexFieldTable.ORDERING;
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
    return getIndexIdentifier();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer value3() {
    return getPosition();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value4() {
    return getCollection();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TableRefTypeT value5() {
    return getTableRef();
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
  public FieldIndexOrdering value7() {
    return getOrdering();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartIndexColumnRecord<TableRefTypeT> value1(String value) {
    setDatabase(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartIndexColumnRecord<TableRefTypeT> value2(String value) {
    setIndexIdentifier(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartIndexColumnRecord<TableRefTypeT> value3(Integer value) {
    setPosition(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartIndexColumnRecord<TableRefTypeT> value4(String value) {
    setCollection(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartIndexColumnRecord<TableRefTypeT> value5(TableRefTypeT value) {
    setTableRef(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartIndexColumnRecord<TableRefTypeT> value6(String value) {
    setIdentifier(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaDocPartIndexColumnRecord<TableRefTypeT> value7(FieldIndexOrdering value) {
    setOrdering(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaDocPartIndexColumnRecord<TableRefTypeT> values(String database,
      String indexIdentifier, Integer position, String collection, TableRefTypeT tableRef,
      String identifier, FieldIndexOrdering fieldIndexOrdering);

  public MetaDocPartIndexColumnRecord<TableRefTypeT> values(String database, String indexIdentifier,
      Integer position, String collection, TableRef tableRef, String identifier,
      FieldIndexOrdering fieldIndexOrdering) {
    return values(database, indexIdentifier, position, collection, toTableRefType(tableRef),
        identifier, fieldIndexOrdering);
  }

  protected abstract TableRefTypeT toTableRefType(TableRef tableRef);

  public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------
  private final MetaDocPartIndexColumnTable<TableRefTypeT, MetaDocPartIndexColumnRecord<TableRefTypeT>> metaIndexFieldTable;

  /**
   * Create a detached MetaFieldRecord
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public MetaDocPartIndexColumnRecord(MetaDocPartIndexColumnTable metaFieldTable) {
    super(metaFieldTable);

    this.metaIndexFieldTable = metaFieldTable;
  }
}
