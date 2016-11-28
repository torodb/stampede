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
import com.torodb.backend.derby.tables.records.DerbyMetaDocPartIndexRecord;
import com.torodb.backend.tables.MetaDocPartIndexTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import javax.json.JsonArray;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class DerbyMetaDocPartIndexTable
    extends MetaDocPartIndexTable<JsonArray, DerbyMetaDocPartIndexRecord> {

  private static final long serialVersionUID = -6245672836069836849L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final DerbyMetaDocPartIndexTable DOC_PART_INDEX = new DerbyMetaDocPartIndexTable();

  @Override
  public Class<DerbyMetaDocPartIndexRecord> getRecordType() {
    return DerbyMetaDocPartIndexRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public DerbyMetaDocPartIndexTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public DerbyMetaDocPartIndexTable(String alias) {
    this(alias, DerbyMetaDocPartIndexTable.DOC_PART_INDEX);
  }

  private DerbyMetaDocPartIndexTable(String alias, Table<DerbyMetaDocPartIndexRecord> aliased) {
    this(alias, aliased, null);
  }

  private DerbyMetaDocPartIndexTable(String alias, Table<DerbyMetaDocPartIndexRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DerbyMetaDocPartIndexTable as(String alias) {
    return new DerbyMetaDocPartIndexTable(alias, this);
  }

  /**
   * Rename this table
   */
  @Override
  public DerbyMetaDocPartIndexTable rename(String name) {
    return new DerbyMetaDocPartIndexTable(name, null);
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexRecord, String> createDatabaseField() {
    return createField(
        TableFields.DATABASE.fieldName,
        SQLDataType.VARCHAR.nullable(false), 
        this,
        "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexRecord, JsonArray> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, JsonArrayConverter.TYPE.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexRecord, Boolean> createUniqueField() {
    return createField(TableFields.UNIQUE.fieldName, SQLDataType.BOOLEAN.nullable(false), this, "");
  }

}
