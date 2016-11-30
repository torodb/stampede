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

import com.torodb.backend.converters.jooq.FieldTypeConverter;
import com.torodb.backend.converters.jooq.OrderingConverter;
import com.torodb.backend.derby.converters.jooq.JsonArrayConverter;
import com.torodb.backend.derby.tables.records.DerbyMetaFieldIndexRecord;
import com.torodb.backend.tables.MetaFieldIndexTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import javax.json.JsonArray;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class DerbyMetaFieldIndexTable
    extends MetaFieldIndexTable<JsonArray, DerbyMetaFieldIndexRecord> {

  private static final long serialVersionUID = -5790340936992206370L;
  /**
   * The singleton instance of <code>torodb.field_index</code>
   */
  public static final DerbyMetaFieldIndexTable FIELD_INDEX = new DerbyMetaFieldIndexTable();

  @Override
  public Class<DerbyMetaFieldIndexRecord> getRecordType() {
    return DerbyMetaFieldIndexRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public DerbyMetaFieldIndexTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public DerbyMetaFieldIndexTable(String alias) {
    this(alias, DerbyMetaFieldIndexTable.FIELD_INDEX);
  }

  private DerbyMetaFieldIndexTable(String alias, Table<DerbyMetaFieldIndexRecord> aliased) {
    this(alias, aliased, null);
  }

  private DerbyMetaFieldIndexTable(String alias, Table<DerbyMetaFieldIndexRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DerbyMetaFieldIndexTable as(String alias) {
    return new DerbyMetaFieldIndexTable(alias, this);
  }

  /**
   * Rename this table
   */
  @Override
  public DerbyMetaFieldIndexTable rename(String name) {
    return new DerbyMetaFieldIndexTable(name, null);
  }

  @Override
  protected TableField<DerbyMetaFieldIndexRecord, String> createDatabaseField() {
    return createField(
        TableFields.DATABASE.fieldName,
        SQLDataType.VARCHAR.nullable(false),
        this,
        "");
  }

  @Override
  protected TableField<DerbyMetaFieldIndexRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaFieldIndexRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaFieldIndexRecord, JsonArray> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, JsonArrayConverter.TYPE.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaFieldIndexRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaFieldIndexRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<DerbyMetaFieldIndexRecord, FieldType> createTypeField() {
    return createField(TableFields.TYPE.fieldName, FieldTypeConverter.TYPE.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaFieldIndexRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this,
        "");
  }

}
