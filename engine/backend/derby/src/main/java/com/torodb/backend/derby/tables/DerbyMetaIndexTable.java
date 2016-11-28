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

import com.torodb.backend.derby.tables.records.DerbyMetaIndexRecord;
import com.torodb.backend.tables.MetaIndexTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class DerbyMetaIndexTable extends MetaIndexTable<DerbyMetaIndexRecord> {

  private static final long serialVersionUID = -7901334417081875840L;
  /**
   * The singleton instance of <code>torodb.index</code>
   */
  public static final DerbyMetaIndexTable INDEX = new DerbyMetaIndexTable();

  @Override
  public Class<DerbyMetaIndexRecord> getRecordType() {
    return DerbyMetaIndexRecord.class;
  }

  /**
   * Create a <code>torodb.index</code> table reference
   */
  public DerbyMetaIndexTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.index</code> table reference
   */
  public DerbyMetaIndexTable(String alias) {
    this(alias, DerbyMetaIndexTable.INDEX);
  }

  private DerbyMetaIndexTable(String alias, Table<DerbyMetaIndexRecord> aliased) {
    this(alias, aliased, null);
  }

  private DerbyMetaIndexTable(String alias, Table<DerbyMetaIndexRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DerbyMetaIndexTable as(String alias) {
    return new DerbyMetaIndexTable(alias, this);
  }

  /**
   * Rename this table
   */
  public DerbyMetaIndexTable rename(String name) {
    return new DerbyMetaIndexTable(name, null);
  }

  @Override
  protected TableField<DerbyMetaIndexRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaIndexRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<DerbyMetaIndexRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<DerbyMetaIndexRecord, Boolean> createUniqueField() {
    return createField(TableFields.UNIQUE.fieldName, SQLDataType.BOOLEAN.nullable(false), this, "");
  }

}
