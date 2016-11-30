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

package com.torodb.backend.postgresql.tables;

import com.torodb.backend.postgresql.tables.records.PostgreSqlMetaCollectionRecord;
import com.torodb.backend.tables.MetaCollectionTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
@SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:AbbreviationAsWordInName",
    "checkstyle:LineLength"})
public class PostgreSqlMetaCollectionTable extends MetaCollectionTable<PostgreSqlMetaCollectionRecord> {

  private static final long serialVersionUID = 304258902776870571L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final PostgreSqlMetaCollectionTable COLLECTION = new PostgreSqlMetaCollectionTable();

  @Override
  public Class<PostgreSqlMetaCollectionRecord> getRecordType() {
    return PostgreSqlMetaCollectionRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaCollectionTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaCollectionTable(String alias) {
    this(alias, PostgreSqlMetaCollectionTable.COLLECTION);
  }

  private PostgreSqlMetaCollectionTable(String alias, Table<PostgreSqlMetaCollectionRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlMetaCollectionTable(String alias, Table<PostgreSqlMetaCollectionRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlMetaCollectionTable as(String alias) {
    return new PostgreSqlMetaCollectionTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlMetaCollectionTable rename(String name) {
    return new PostgreSqlMetaCollectionTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlMetaCollectionRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaCollectionRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaCollectionRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }
}
