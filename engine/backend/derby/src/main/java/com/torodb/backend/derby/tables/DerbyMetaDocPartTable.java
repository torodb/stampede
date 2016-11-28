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

package com.torodb.backend.derby.tables;

import com.torodb.backend.derby.converters.jooq.JsonArrayConverter;
import com.torodb.backend.derby.tables.records.DerbyMetaDocPartRecord;
import com.torodb.backend.tables.MetaDocPartTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import javax.json.JsonArray;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class DerbyMetaDocPartTable extends MetaDocPartTable<JsonArray, DerbyMetaDocPartRecord> {

  private static final long serialVersionUID = -550698624070753099L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final DerbyMetaDocPartTable DOC_PART = new DerbyMetaDocPartTable();

  @Override
  public Class<DerbyMetaDocPartRecord> getRecordType() {
    return DerbyMetaDocPartRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public DerbyMetaDocPartTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public DerbyMetaDocPartTable(String alias) {
    this(alias, DerbyMetaDocPartTable.DOC_PART);
  }

  private DerbyMetaDocPartTable(String alias, Table<DerbyMetaDocPartRecord> aliased) {
    this(alias, aliased, null);
  }

  private DerbyMetaDocPartTable(String alias, Table<DerbyMetaDocPartRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DerbyMetaDocPartTable as(String alias) {
    return new DerbyMetaDocPartTable(alias, this);
  }

  /**
   * Rename this table
   */
  public DerbyMetaDocPartTable rename(String name) {
    return new DerbyMetaDocPartTable(name, null);
  }

  @Override
  protected TableField<DerbyMetaDocPartRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaDocPartRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaDocPartRecord, JsonArray> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, JsonArrayConverter.TYPE.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaDocPartRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaDocPartRecord, Integer> createLastRidField() {
    return createField(TableFields.LAST_RID.fieldName, SQLDataType.INTEGER.nullable(false),
        this, "");
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
