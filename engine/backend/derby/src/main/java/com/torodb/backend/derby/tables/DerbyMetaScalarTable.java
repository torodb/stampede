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
import com.torodb.backend.derby.converters.jooq.JsonArrayConverter;
import com.torodb.backend.derby.tables.records.DerbyMetaScalarRecord;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.transaction.metainf.FieldType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import javax.json.JsonArray;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class DerbyMetaScalarTable extends MetaScalarTable<JsonArray, DerbyMetaScalarRecord> {

  private static final long serialVersionUID = -4901972508689222558L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final DerbyMetaScalarTable SCALAR = new DerbyMetaScalarTable();

  @Override
  public Class<DerbyMetaScalarRecord> getRecordType() {
    return DerbyMetaScalarRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public DerbyMetaScalarTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public DerbyMetaScalarTable(String alias) {
    this(alias, DerbyMetaScalarTable.SCALAR);
  }

  private DerbyMetaScalarTable(String alias, Table<DerbyMetaScalarRecord> aliased) {
    this(alias, aliased, null);
  }

  private DerbyMetaScalarTable(String alias, Table<DerbyMetaScalarRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DerbyMetaScalarTable as(String alias) {
    return new DerbyMetaScalarTable(alias, this);
  }

  /**
   * Rename this table
   */
  public DerbyMetaScalarTable rename(String name) {
    return new DerbyMetaScalarTable(name, null);
  }

  @Override
  protected TableField<DerbyMetaScalarRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaScalarRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaScalarRecord, JsonArray> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, JsonArrayConverter.TYPE.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaScalarRecord, FieldType> createTypeField() {
    return createField(TableFields.TYPE.fieldName, FieldTypeConverter.TYPE.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaScalarRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }
}
