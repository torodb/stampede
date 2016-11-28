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
import com.torodb.backend.derby.tables.records.DerbyMetaIndexFieldRecord;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import javax.json.JsonArray;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class DerbyMetaIndexFieldTable
    extends MetaIndexFieldTable<JsonArray, DerbyMetaIndexFieldRecord> {

  private static final long serialVersionUID = -8622370359242191974L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final DerbyMetaIndexFieldTable INDEX_FIELD = new DerbyMetaIndexFieldTable();

  @Override
  public Class<DerbyMetaIndexFieldRecord> getRecordType() {
    return DerbyMetaIndexFieldRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public DerbyMetaIndexFieldTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public DerbyMetaIndexFieldTable(String alias) {
    this(alias, DerbyMetaIndexFieldTable.INDEX_FIELD);
  }

  private DerbyMetaIndexFieldTable(String alias, Table<DerbyMetaIndexFieldRecord> aliased) {
    this(alias, aliased, null);
  }

  private DerbyMetaIndexFieldTable(String alias, Table<DerbyMetaIndexFieldRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DerbyMetaIndexFieldTable as(String alias) {
    return new DerbyMetaIndexFieldTable(alias, this);
  }

  /**
   * Rename this table
   */
  public DerbyMetaIndexFieldTable rename(String name) {
    return new DerbyMetaIndexFieldTable(name, null);
  }

  @Override
  protected TableField<DerbyMetaIndexFieldRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaIndexFieldRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaIndexFieldRecord, String> createIndexField() {
    return createField(TableFields.INDEX.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<DerbyMetaIndexFieldRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaIndexFieldRecord, JsonArray> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, JsonArrayConverter.TYPE.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaIndexFieldRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<DerbyMetaIndexFieldRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this,
        "");
  }
}
