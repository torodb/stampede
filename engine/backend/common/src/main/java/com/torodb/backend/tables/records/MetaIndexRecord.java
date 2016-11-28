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

import com.torodb.backend.tables.MetaIndexTable;
import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Record4;
import org.jooq.Row4;
import org.jooq.impl.UpdatableRecordImpl;

@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public abstract class MetaIndexRecord extends UpdatableRecordImpl<MetaIndexRecord>
    implements Record4<String, String, String, Boolean> {

  private static final long serialVersionUID = -567809380986685830L;

  /**
   * Setter for <code>torodb.index.database</code>.
   */
  public void setDatabase(String value) {
    set(0, value);
  }

  /**
   * Getter for <code>torodb.index.database</code>.
   */
  public String getDatabase() {
    return (String) getValue(0);
  }

  /**
   * Setter for <code>torodb.index.collection</code>.
   */
  public void setCollection(String value) {
    set(1, value);
  }

  /**
   * Getter for <code>torodb.index.collection</code>.
   */
  public String getCollection() {
    return (String) getValue(1);
  }

  /**
   * Setter for <code>torodb.index.name</code>.
   */
  public void setName(String value) {
    set(2, value);
  }

  /**
   * Getter for <code>torodb.index.name</code>.
   */
  public String getName() {
    return (String) getValue(2);
  }

  /**
   * Setter for <code>torodb.index.unique</code>.
   */
  public void setUnique(Boolean value) {
    set(3, value);
  }

  /**
   * Getter for <code>torodb.index.unique</code>.
   */
  public Boolean getUnique() {
    return (Boolean) getValue(3);
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
  public Row4<String, String, String, Boolean> fieldsRow() {
    return (Row4<String, String, String, Boolean>) super.fieldsRow();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row4<String, String, String, Boolean> valuesRow() {
    return (Row4<String, String, String, Boolean>) super.valuesRow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field1() {
    return metaIndexTable.DATABASE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field2() {
    return metaIndexTable.COLLECTION;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field3() {
    return metaIndexTable.NAME;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<Boolean> field4() {
    return metaIndexTable.UNIQUE;
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
  public String value3() {
    return getName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Boolean value4() {
    return getUnique();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaIndexRecord value1(String value) {
    setDatabase(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaIndexRecord value2(String value) {
    setCollection(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaIndexRecord value3(String value) {
    setName(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaIndexRecord value4(Boolean value) {
    setUnique(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaIndexRecord values(String database, String collection, String name,
      Boolean unique);

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------
  private final MetaIndexTable<MetaIndexRecord> metaIndexTable;

  /**
   * Create a detached MetaIndexRecord
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public MetaIndexRecord(MetaIndexTable metaIndexTable) {
    super(metaIndexTable);

    this.metaIndexTable = metaIndexTable;
  }
}
