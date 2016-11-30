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

import com.torodb.backend.postgresql.tables.records.PostgreSqlMetaDatabaseRecord;
import com.torodb.backend.tables.MetaDatabaseTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class PostgreSqlMetaDatabaseTable extends MetaDatabaseTable<PostgreSqlMetaDatabaseRecord> {

  private static final long serialVersionUID = -5506554761865128847L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final PostgreSqlMetaDatabaseTable DATABASE = new PostgreSqlMetaDatabaseTable();

  @Override
  public Class<PostgreSqlMetaDatabaseRecord> getRecordType() {
    return PostgreSqlMetaDatabaseRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaDatabaseTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaDatabaseTable(String alias) {
    this(alias, PostgreSqlMetaDatabaseTable.DATABASE);
  }

  private PostgreSqlMetaDatabaseTable(String alias, Table<PostgreSqlMetaDatabaseRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlMetaDatabaseTable(String alias, Table<PostgreSqlMetaDatabaseRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlMetaDatabaseTable as(String alias) {
    return new PostgreSqlMetaDatabaseTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlMetaDatabaseTable rename(String name) {
    return new PostgreSqlMetaDatabaseTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlMetaDatabaseRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDatabaseRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }
}
