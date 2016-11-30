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

import com.torodb.backend.postgresql.tables.records.PostgreSqlMetaDocPartIndexRecord;
import com.torodb.backend.tables.MetaDocPartIndexTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class PostgreSqlMetaDocPartIndexTable
    extends MetaDocPartIndexTable<String[], PostgreSqlMetaDocPartIndexRecord> {

  private static final long serialVersionUID = 1726883639731937990L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final PostgreSqlMetaDocPartIndexTable DOC_PART_INDEX =
      new PostgreSqlMetaDocPartIndexTable();

  @Override
  public Class<PostgreSqlMetaDocPartIndexRecord> getRecordType() {
    return PostgreSqlMetaDocPartIndexRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaDocPartIndexTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaDocPartIndexTable(String alias) {
    this(alias, PostgreSqlMetaDocPartIndexTable.DOC_PART_INDEX);
  }

  private PostgreSqlMetaDocPartIndexTable(String alias,
      Table<PostgreSqlMetaDocPartIndexRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlMetaDocPartIndexTable(String alias,
      Table<PostgreSqlMetaDocPartIndexRecord> aliased, Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlMetaDocPartIndexTable as(String alias) {
    return new PostgreSqlMetaDocPartIndexTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlMetaDocPartIndexTable rename(String name) {
    return new PostgreSqlMetaDocPartIndexTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexRecord, String[]> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType()
        .nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartIndexRecord, Boolean> createUniqueField() {
    return createField(TableFields.UNIQUE.fieldName, SQLDataType.BOOLEAN.nullable(false), this, "");
  }

}
