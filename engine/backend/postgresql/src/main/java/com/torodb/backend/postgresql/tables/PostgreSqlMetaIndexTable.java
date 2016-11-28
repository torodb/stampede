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

import com.torodb.backend.postgresql.tables.records.PostgreSqlMetaIndexRecord;
import com.torodb.backend.tables.MetaIndexTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class PostgreSqlMetaIndexTable extends MetaIndexTable<PostgreSqlMetaIndexRecord> {

  private static final long serialVersionUID = -6090026713335495681L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final PostgreSqlMetaIndexTable INDEX = new PostgreSqlMetaIndexTable();

  @Override
  public Class<PostgreSqlMetaIndexRecord> getRecordType() {
    return PostgreSqlMetaIndexRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaIndexTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaIndexTable(String alias) {
    this(alias, PostgreSqlMetaIndexTable.INDEX);
  }

  private PostgreSqlMetaIndexTable(String alias, Table<PostgreSqlMetaIndexRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlMetaIndexTable(String alias, Table<PostgreSqlMetaIndexRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlMetaIndexTable as(String alias) {
    return new PostgreSqlMetaIndexTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlMetaIndexTable rename(String name) {
    return new PostgreSqlMetaIndexTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlMetaIndexRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexRecord, Boolean> createUniqueField() {
    return createField(TableFields.UNIQUE.fieldName, SQLDataType.BOOLEAN.nullable(false), this, "");
  }

}
