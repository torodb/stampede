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

import com.torodb.backend.converters.jooq.FieldTypeConverter;
import com.torodb.backend.converters.jooq.OrderingConverter;
import com.torodb.backend.postgresql.tables.records.PostgreSqlMetaFieldIndexRecord;
import com.torodb.backend.tables.MetaFieldIndexTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
@SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:AbbreviationAsWordInName",
    "checkstyle:LineLength"})
public class PostgreSqlMetaFieldIndexTable
    extends MetaFieldIndexTable<String[], PostgreSqlMetaFieldIndexRecord> {

  private static final long serialVersionUID = -426812622031112992L;
  /**
   * The singleton instance of <code>torodb.field_index</code>
   */
  public static final PostgreSqlMetaFieldIndexTable FIELD_INDEX =
      new PostgreSqlMetaFieldIndexTable();

  @Override
  public Class<PostgreSqlMetaFieldIndexRecord> getRecordType() {
    return PostgreSqlMetaFieldIndexRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaFieldIndexTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaFieldIndexTable(String alias) {
    this(alias, PostgreSqlMetaFieldIndexTable.FIELD_INDEX);
  }

  private PostgreSqlMetaFieldIndexTable(String alias, Table<PostgreSqlMetaFieldIndexRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlMetaFieldIndexTable(String alias, Table<PostgreSqlMetaFieldIndexRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlMetaFieldIndexTable as(String alias) {
    return new PostgreSqlMetaFieldIndexTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlMetaFieldIndexTable rename(String name) {
    return new PostgreSqlMetaFieldIndexTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlMetaFieldIndexRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaFieldIndexRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaFieldIndexRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaFieldIndexRecord, String[]> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType()
        .nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaFieldIndexRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaFieldIndexRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaFieldIndexRecord, FieldType> createTypeField() {
    return createField(TableFields.TYPE.fieldName, FieldTypeConverter.TYPE.nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaFieldIndexRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this,
        "");
  }

}
