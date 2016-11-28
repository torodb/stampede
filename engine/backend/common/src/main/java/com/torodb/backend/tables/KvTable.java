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

package com.torodb.backend.tables;

import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.KvRecord;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "MemberName"})
public abstract class KvTable<R extends KvRecord> extends SemanticTable<R> {

  private static final long serialVersionUID = -8840058751911188345L;

  public static final String TABLE_NAME = "kv";

  public enum TableFields {
    KEY("key"),
    VALUE("value");

    public final String fieldName;

    TableFields(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String toString() {
      return fieldName;
    }
  }

  /**
   * The class holding records for this type
   *
   * @return
   */
  @Override
  public abstract Class<R> getRecordType();

  /**
   * The column <code>torodb.kv.key</code>.
   */
  public final TableField<R, String> KEY = createNameField();

  /**
   * The column <code>torodb.kv.value</code>.
   */
  public final TableField<R, String> VALUE =
      createIdentifierField();

  protected abstract TableField<R, String> createNameField();

  protected abstract TableField<R, String> createIdentifierField();

  private final UniqueKeys<R> uniqueKeys;

  /**
   * Create a <code>torodb.kv</code> table reference
   */
  public KvTable() {
    this(TABLE_NAME, null);
  }

  protected KvTable(String alias, Table<R> aliased) {
    this(alias, aliased, null);
  }

  protected KvTable(String alias, Table<R> aliased, Field<?>[] parameters) {
    super(alias, TorodbSchema.TORODB, aliased, parameters, "");

    this.uniqueKeys = new UniqueKeys<R>(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UniqueKey<R> getPrimaryKey() {
    return uniqueKeys.DATABASE_PKEY;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<UniqueKey<R>> getKeys() {
    return Arrays.<UniqueKey<R>>asList(uniqueKeys.DATABASE_PKEY,
        uniqueKeys.DATABASE_SCHEMA_UNIQUE
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract KvTable<R> as(String alias);

  /**
   * Rename this table
   */
  public abstract KvTable<R> rename(String name);

  public UniqueKeys<R> getUniqueKeys() {
    return uniqueKeys;
  }

  @SuppressWarnings({"checkstyle:LineLength", "checkstyle:AbbreviationAsWordInName",
      "checkstyle:MemberName"})
  public static class UniqueKeys<KeyRecordT extends KvRecord> extends AbstractKeys {

    private final UniqueKey<KeyRecordT> DATABASE_PKEY;
    private final UniqueKey<KeyRecordT> DATABASE_SCHEMA_UNIQUE;

    private UniqueKeys(KvTable<KeyRecordT> databaseTable) {
      DATABASE_PKEY = createUniqueKey(databaseTable, databaseTable.KEY);
      DATABASE_SCHEMA_UNIQUE = createUniqueKey(databaseTable, databaseTable.VALUE);
    }
  }
}
