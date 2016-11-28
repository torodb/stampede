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

import com.torodb.backend.tables.MetaCollectionTable;
import org.jooq.Field;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Row3;
import org.jooq.impl.UpdatableRecordImpl;

@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public abstract class MetaCollectionRecord extends UpdatableRecordImpl<MetaCollectionRecord>
    implements Record3<String, String, String> {

  private static final long serialVersionUID = -2107968478;

  /**
   * Setter for <code>torodb.collection.database</code>.
   */
  public void setDatabase(String value) {
    set(0, value);
  }

  /**
   * Getter for <code>torodb.collection.database</code>.
   */
  public String getDatabase() {
    return (String) getValue(0);
  }

  /**
   * Setter for <code>torodb.collection.name</code>.
   */
  public void setName(String value) {
    set(1, value);
  }

  /**
   * Getter for <code>torodb.collection.name</code>.
   */
  public String getName() {
    return (String) getValue(1);
  }

  /**
   * Getter for <code>torodb.collection.identifier</code>.
   */
  public String getIdentifier() {
    return (String) getValue(2);
  }

  /**
   * Setter for <code>torodb.collection.identifier</code>.
   */
  public void setIdentifier(String value) {
    set(2, value);
  }

  // -------------------------------------------------------------------------
  // Primary key information
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Record2<String, String> key() {
    return (Record2<String, String>) super.key();
  }

  // -------------------------------------------------------------------------
  // Record7 type implementation
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row3<String, String, String> fieldsRow() {
    return (Row3<String, String, String>) super.fieldsRow();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row3<String, String, String> valuesRow() {
    return (Row3<String, String, String>) super.valuesRow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field1() {
    return metaCollectionTable.DATABASE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field2() {
    return metaCollectionTable.NAME;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field3() {
    return metaCollectionTable.IDENTIFIER;
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
    return getName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value3() {
    return getIdentifier();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaCollectionRecord value1(String value) {
    setDatabase(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaCollectionRecord value2(String value) {
    setName(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MetaCollectionRecord value3(String value) {
    setIdentifier(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaCollectionRecord values(String database, String name, String identifier);

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------
  private final MetaCollectionTable<MetaCollectionRecord> metaCollectionTable;

  /**
   * Create a detached MetaCollectionRecord
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public MetaCollectionRecord(MetaCollectionTable metaCollectionTable) {
    super(metaCollectionTable);

    this.metaCollectionTable = metaCollectionTable;
  }
}
