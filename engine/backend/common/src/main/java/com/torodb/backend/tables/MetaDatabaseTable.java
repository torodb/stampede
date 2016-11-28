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

package com.torodb.backend.tables;

import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;

import java.util.Arrays;
import java.util.List;

@SuppressFBWarnings(
    value = "HE_HASHCODE_NO_EQUALS",
    justification =
    "Equals comparation is done in TableImpl class, which compares schema, name and fields")
@SuppressWarnings({"checkstyle:LineLength", "checkstyle:AbbreviationAsWordInName",
    "checkstyle:MemberName"})
public abstract class MetaDatabaseTable<R extends MetaDatabaseRecord> extends SemanticTable<R> {

  private static final long serialVersionUID = -8840058751911188345L;

  public static final String TABLE_NAME = "database";

  public enum TableFields {
    NAME("name"),
    IDENTIFIER("identifier");

    public final String fieldName;

    TableFields(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String toString() {
      return fieldName;
    }
  }

  /**
   * The class holding records for this type
   *
   * @return
   */
  @Override
  public abstract Class<R> getRecordType();

  /**
   * The column <code>torodb.database.name</code>.
   */
  public final TableField<R, String> NAME =
      createNameField();

  /**
   * The column <code>torodb.database.schema</code>.
   */
  public final TableField<R, String> IDENTIFIER =
      createIdentifierField();

  protected abstract TableField<R, String> createNameField();

  protected abstract TableField<R, String> createIdentifierField();

  private final UniqueKeys<R> uniqueKeys;

  /**
   * Create a <code>torodb.database</code> table reference
   */
  public MetaDatabaseTable() {
    this(TABLE_NAME, null);
  }

  protected MetaDatabaseTable(String alias, Table<R> aliased) {
    this(alias, aliased, null);
  }

  protected MetaDatabaseTable(String alias, Table<R> aliased, Field<?>[] parameters) {
    super(alias, TorodbSchema.TORODB, aliased, parameters, "");

    this.uniqueKeys = new UniqueKeys<>(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UniqueKey<R> getPrimaryKey() {
    return uniqueKeys.DATABASE_PKEY;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<UniqueKey<R>> getKeys() {
    return Arrays.<UniqueKey<R>>asList(uniqueKeys.DATABASE_PKEY,
        uniqueKeys.DATABASE_SCHEMA_UNIQUE
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaDatabaseTable<R> as(String alias);

  /**
   * Rename this table
   */
  public abstract MetaDatabaseTable<R> rename(String name);

  public UniqueKeys<R> getUniqueKeys() {
    return uniqueKeys;
  }

  @SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:MemberName"})
  public static class UniqueKeys<KeyRecordT extends MetaDatabaseRecord> extends AbstractKeys {

    private final UniqueKey<KeyRecordT> DATABASE_PKEY;
    private final UniqueKey<KeyRecordT> DATABASE_SCHEMA_UNIQUE;

    private UniqueKeys(MetaDatabaseTable<KeyRecordT> databaseTable) {
      DATABASE_PKEY = createUniqueKey(databaseTable, databaseTable.NAME);
      DATABASE_SCHEMA_UNIQUE = createUniqueKey(databaseTable, databaseTable.IDENTIFIER);
    }
  }
}
