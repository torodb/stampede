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

import com.torodb.backend.postgresql.tables.records.PostgreSqlKvRecord;
import com.torodb.backend.tables.KvTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class PostgreSqlKvTable extends KvTable<PostgreSqlKvRecord> {

  private static final long serialVersionUID = -5506554761865128847L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final PostgreSqlKvTable KV = new PostgreSqlKvTable();

  @Override
  public Class<PostgreSqlKvRecord> getRecordType() {
    return PostgreSqlKvRecord.class;
  }

  public PostgreSqlKvTable() {
    this(TABLE_NAME, null);
  }

  public PostgreSqlKvTable(String alias) {
    this(alias, PostgreSqlKvTable.KV);
  }

  private PostgreSqlKvTable(String alias, Table<PostgreSqlKvRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlKvTable(
      String alias,
      Table<PostgreSqlKvRecord> aliased, Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlKvTable as(String alias) {
    return new PostgreSqlKvTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlKvTable rename(String name) {
    return new PostgreSqlKvTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlKvRecord, String> createNameField() {
    return createField(TableFields.KEY.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlKvRecord, String> createIdentifierField() {
    return createField(TableFields.VALUE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }
}
