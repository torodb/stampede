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

import com.torodb.backend.tables.KvTable;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Row2;
import org.jooq.impl.UpdatableRecordImpl;

@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public abstract class KvRecord extends UpdatableRecordImpl<KvRecord>
    implements Record2<String, String> {

  private static final long serialVersionUID = -3134779659016002480L;

  /**
   * Setter for <code>torodb.kv.name</code>.
   */
  public void setKey(String value) {
    set(0, value);
  }

  /**
   * Getter for <code>torodb.kv.name</code>.
   */
  public String getKey() {
    return (String) getValue(0);
  }

  /**
   * Setter for <code>torodb.kv.identifier</code>.
   */
  public void setValue(String value) {
    set(1, value);
  }

  /**
   * Getter for <code>torodb.kv.identifier</code>.
   */
  public String getValue() {
    return (String) getValue(1);
  }

  // -------------------------------------------------------------------------
  // Primary key information
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Record1<String> key() {
    return (Record1<String>) super.key();
  }

  // -------------------------------------------------------------------------
  // Record7 type implementation
  // -------------------------------------------------------------------------
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row2<String, String> fieldsRow() {
    return (Row2<String, String>) super.fieldsRow();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row2<String, String> valuesRow() {
    return (Row2<String, String>) super.valuesRow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field1() {
    return kvTable.KEY;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Field<String> field2() {
    return kvTable.VALUE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value1() {
    return getKey();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String value2() {
    return getValue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KvRecord value1(String value) {
    setKey(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KvRecord value2(String value) {
    setValue(value);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract KvRecord values(String name, String identifier);

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------
  private final KvTable<KvRecord> kvTable;

  /**
   * Create a detached MetainfRecord
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public KvRecord(KvTable kvTable) {
    super(kvTable);

    this.kvTable = kvTable;
  }
}
