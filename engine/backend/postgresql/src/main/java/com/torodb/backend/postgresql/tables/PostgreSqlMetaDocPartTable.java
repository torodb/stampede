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

import com.torodb.backend.postgresql.tables.records.PostgreSqlMetaDocPartRecord;
import com.torodb.backend.tables.MetaDocPartTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.util.postgres.PostgresDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class PostgreSqlMetaDocPartTable
    extends MetaDocPartTable<String[], PostgreSqlMetaDocPartRecord> {

  private static final long serialVersionUID = -550698624070753099L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final PostgreSqlMetaDocPartTable DOC_PART = new PostgreSqlMetaDocPartTable();

  @Override
  public Class<PostgreSqlMetaDocPartRecord> getRecordType() {
    return PostgreSqlMetaDocPartRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaDocPartTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public PostgreSqlMetaDocPartTable(String alias) {
    this(alias, PostgreSqlMetaDocPartTable.DOC_PART);
  }

  private PostgreSqlMetaDocPartTable(String alias, Table<PostgreSqlMetaDocPartRecord> aliased) {
    this(alias, aliased, null);
  }

  private PostgreSqlMetaDocPartTable(String alias, Table<PostgreSqlMetaDocPartRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PostgreSqlMetaDocPartTable as(String alias) {
    return new PostgreSqlMetaDocPartTable(alias, this);
  }

  /**
   * Rename this table
   */
  public PostgreSqlMetaDocPartTable rename(String name) {
    return new PostgreSqlMetaDocPartTable(name, null);
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartRecord, String[]> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType()
        .nullable(false), this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, PostgresDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<PostgreSqlMetaDocPartRecord, Integer> createLastRidField() {
    return createField(TableFields.LAST_RID.fieldName, SQLDataType.INTEGER.nullable(false), this,
        "");
  }

  @Override
  protected Field<Integer> createDidField() {
    return DSL.field(DocPartTableFields.DID.fieldName, SQLDataType.INTEGER.nullable(false));
  }

  @Override
  protected Field<Integer> createRidField() {
    return DSL.field(DocPartTableFields.RID.fieldName, SQLDataType.INTEGER.nullable(false));
  }

  @Override
  protected Field<Integer> createPidField() {
    return DSL.field(DocPartTableFields.PID.fieldName, SQLDataType.INTEGER.nullable(false));
  }

  @Override
  protected Field<Integer> createSeqField() {
    return DSL.field(DocPartTableFields.SEQ.fieldName, SQLDataType.INTEGER.nullable(false));
  }
}
