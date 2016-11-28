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

import com.torodb.backend.derby.tables.records.DerbyMetaCollectionRecord;
import com.torodb.backend.tables.MetaCollectionTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class DerbyMetaCollectionTable extends MetaCollectionTable<DerbyMetaCollectionRecord> {

  private static final long serialVersionUID = 304258902776870571L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final DerbyMetaCollectionTable COLLECTION = new DerbyMetaCollectionTable();

  @Override
  public Class<DerbyMetaCollectionRecord> getRecordType() {
    return DerbyMetaCollectionRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public DerbyMetaCollectionTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public DerbyMetaCollectionTable(String alias) {
    this(alias, DerbyMetaCollectionTable.COLLECTION);
  }

  private DerbyMetaCollectionTable(String alias, Table<DerbyMetaCollectionRecord> aliased) {
    this(alias, aliased, null);
  }

  private DerbyMetaCollectionTable(String alias, Table<DerbyMetaCollectionRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DerbyMetaCollectionTable as(String alias) {
    return new DerbyMetaCollectionTable(alias, this);
  }

  /**
   * Rename this table
   */
  public DerbyMetaCollectionTable rename(String name) {
    return new DerbyMetaCollectionTable(name, null);
  }

  @Override
  protected TableField<DerbyMetaCollectionRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<DerbyMetaCollectionRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<DerbyMetaCollectionRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }
}
