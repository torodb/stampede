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

import com.torodb.backend.converters.jooq.OrderingConverter;
import com.torodb.backend.derby.converters.jooq.JsonArrayConverter;
import com.torodb.backend.derby.tables.records.DerbyMetaDocPartIndexColumnRecord;
import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import javax.json.JsonArray;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class DerbyMetaDocPartIndexColumnTable
    extends MetaDocPartIndexColumnTable<JsonArray, DerbyMetaDocPartIndexColumnRecord> {

  private static final long serialVersionUID = -5790340936992206370L;
  /**
   * The singleton instance of <code>torodb.field_index</code>
   */
  public static final DerbyMetaDocPartIndexColumnTable DOC_PART_INDEX_COLUMN =
      new DerbyMetaDocPartIndexColumnTable();

  @Override
  public Class<DerbyMetaDocPartIndexColumnRecord> getRecordType() {
    return DerbyMetaDocPartIndexColumnRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public DerbyMetaDocPartIndexColumnTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public DerbyMetaDocPartIndexColumnTable(String alias) {
    this(alias, DerbyMetaDocPartIndexColumnTable.DOC_PART_INDEX_COLUMN);
  }

  private DerbyMetaDocPartIndexColumnTable(String alias,
      Table<DerbyMetaDocPartIndexColumnRecord> aliased) {
    this(alias, aliased, null);
  }

  private DerbyMetaDocPartIndexColumnTable(String alias,
      Table<DerbyMetaDocPartIndexColumnRecord> aliased, Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DerbyMetaDocPartIndexColumnTable as(String alias) {
    return new DerbyMetaDocPartIndexColumnTable(alias, this);
  }

  /**
   * Rename this table
   */
  public DerbyMetaDocPartIndexColumnTable rename(String name) {
    return new DerbyMetaDocPartIndexColumnTable(name, null);
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexColumnRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexColumnRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexColumnRecord, String> createIndexIdentifierField() {
    return createField(TableFields.INDEX_IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexColumnRecord, JsonArray> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, JsonArrayConverter.TYPE.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexColumnRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaDocPartIndexColumnRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  @SuppressWarnings("checkstyle:LineLength")
  protected TableField<DerbyMetaDocPartIndexColumnRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), 
        this, "");
  }

}
