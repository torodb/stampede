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
import com.torodb.backend.postgresql.tables.records.PostgreSqlMetaDocPartIndexColumnRecord;
import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class PostgreSqlMetaDocPartIndexColumnTable 
    extends MetaDocPartIndexColumnTable<String[], PostgreSqlMetaDocPartIndexColumnRecord> {

  private static final long serialVersionUID = -426812622031112992L;
  /**
   * The singleton instance of <code>torodb.field_index</code>
   */
  public static final PostgreSqlMetaDocPartIndexColumnTable DOC_PART_INDEX_COLUMN =
      new PostgreSqlMetaDocPartIndexColumnTable();

  @Override
  public Class<PostgreSqlMetaDocPartIndexColumnRecord> getRecordType() {
    return PostgreSqlMetaDocPartIndexColumnRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaDocPartIndexColumnTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaDocPartIndexColumnTable(String alias) {
    this(alias, PostgreSqlMetaDocPartIndexColumnTable.DOC_PART_INDEX_COLUMN);
  }

  private PostgreSqlMetaDocPartIndexColumnTable(String alias,
      Table<PostgreSqlMetaDocPartIndexColumnRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlMetaDocPartIndexColumnTable(String alias,
      Table<PostgreSqlMetaDocPartIndexColumnRecord> aliased, Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlMetaDocPartIndexColumnTable as(String alias) {
    return new PostgreSqlMetaDocPartIndexColumnTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlMetaDocPartIndexColumnTable rename(String name) {
    return new PostgreSqlMetaDocPartIndexColumnTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexColumnRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexColumnRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  @SuppressWarnings("checkstyle:LineLength")
  protected TableField<PostgreSqlMetaDocPartIndexColumnRecord, String> createIndexIdentifierField() {
    return createField(TableFields.INDEX_IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexColumnRecord, String[]> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType()
        .nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexColumnRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false),
        this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexColumnRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  @SuppressWarnings("checkstyle:LineLength")
  protected TableField<PostgreSqlMetaDocPartIndexColumnRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this,
        "");
  }

}
