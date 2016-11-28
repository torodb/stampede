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

import com.torodb.backend.converters.jooq.OrderingConverter;
import com.torodb.backend.postgresql.tables.records.PostgreSqlMetaIndexFieldRecord;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
@SuppressWarnings("checkstyle:LineLength")
public class PostgreSqlMetaIndexFieldTable extends MetaIndexFieldTable<String[], PostgreSqlMetaIndexFieldRecord> {

  private static final long serialVersionUID = 8649935905000022435L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final PostgreSqlMetaIndexFieldTable INDEX_FIELD =
      new PostgreSqlMetaIndexFieldTable();

  @Override
  public Class<PostgreSqlMetaIndexFieldRecord> getRecordType() {
    return PostgreSqlMetaIndexFieldRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaIndexFieldTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaIndexFieldTable(String alias) {
    this(alias, PostgreSqlMetaIndexFieldTable.INDEX_FIELD);
  }

  private PostgreSqlMetaIndexFieldTable(String alias, Table<PostgreSqlMetaIndexFieldRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlMetaIndexFieldTable(String alias, Table<PostgreSqlMetaIndexFieldRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlMetaIndexFieldTable as(String alias) {
    return new PostgreSqlMetaIndexFieldTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlMetaIndexFieldTable rename(String name) {
    return new PostgreSqlMetaIndexFieldTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlMetaIndexFieldRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexFieldRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexFieldRecord, String> createIndexField() {
    return createField(TableFields.INDEX.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexFieldRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexFieldRecord, String[]> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType()
        .nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexFieldRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaIndexFieldRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this,
        "");
  }
}
