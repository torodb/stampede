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
import com.torodb.backend.tables.records.MetaFieldIndexRecord;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
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
public abstract class MetaFieldIndexTable<TableRefTypeT, R extends MetaFieldIndexRecord<TableRefTypeT>>
    extends SemanticTable<R> {

  private static final long serialVersionUID = 2116451834151644607L;

  public static final String TABLE_NAME = "field_index";

  public enum TableFields {
    DATABASE("database"),
    IDENTIFIER("identifier"),
    POSITION("position"),
    COLLECTION("collection"),
    TABLE_REF("table_ref"),
    NAME("name"),
    TYPE("type"),
    ORDERING("ordering");

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
   * The column <code>torodb.index_field.database</code>.
   */
  public final TableField<R, String> DATABASE =
      createDatabaseField();

  /**
   * The column <code>torodb.index_field.identifier</code>.
   */
  public final TableField<R, String> IDENTIFIER =
      createIdentifierField();

  /**
   * The column <code>torodb.index_field.position</code>.
   */
  public final TableField<R, Integer> POSITION =
      createPositionField();

  /**
   * The column <code>torodb.index_field.collection</code>.
   */
  public final TableField<R, String> COLLECTION =
      createCollectionField();

  /**
   * The column <code>torodb.index_field.path</code>.
   */
  public final TableField<R, TableRefTypeT> TABLE_REF =
      createTableRefField();

  /**
   * The column <code>torodb.index_field.name</code>.
   */
  public final TableField<R, String> NAME =
      createNameField();

  /**
   * The column <code>torodb.index_field.position</code>.
   */
  public final TableField<R, FieldType> TYPE =
      createTypeField();

  /**
   * The column <code>torodb.index_field.ordering</code>.
   */
  public final TableField<R, FieldIndexOrdering> ORDERING =
      createOrderingField();

  protected abstract TableField<R, String> createDatabaseField();

  protected abstract TableField<R, String> createIdentifierField();

  protected abstract TableField<R, Integer> createPositionField();

  protected abstract TableField<R, String> createCollectionField();

  protected abstract TableField<R, TableRefTypeT> createTableRefField();

  protected abstract TableField<R, String> createNameField();

  protected abstract TableField<R, FieldType> createTypeField();

  protected abstract TableField<R, FieldIndexOrdering> createOrderingField();

  private final UniqueKeys<TableRefTypeT, R> uniqueKeys;

  /**
   * Create a <code>torodb.index_field</code> table reference
   */
  public MetaFieldIndexTable() {
    this(TABLE_NAME, null);
  }

  protected MetaFieldIndexTable(String alias, Table<R> aliased) {
    this(alias, aliased, null);
  }

  protected MetaFieldIndexTable(String alias, Table<R> aliased, Field<?>[] parameters) {
    super(alias, TorodbSchema.TORODB, aliased, parameters, "");

    this.uniqueKeys = new UniqueKeys<TableRefTypeT, R>(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UniqueKey<R> getPrimaryKey() {
    return uniqueKeys.FIELD_INDEX_PKEY;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<UniqueKey<R>> getKeys() {
    return Arrays.<UniqueKey<R>>asList(uniqueKeys.FIELD_INDEX_PKEY);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaFieldIndexTable<TableRefTypeT, R> as(String alias);

  /**
   * Rename this table
   */
  public abstract MetaFieldIndexTable<TableRefTypeT, R> rename(String name);

  public UniqueKeys<TableRefTypeT, R> getUniqueKeys() {
    return uniqueKeys;
  }

  @SuppressWarnings("checkstyle:LineLength")
  public static class UniqueKeys<TableRefTypeT, KeyRecordT extends MetaFieldIndexRecord<TableRefTypeT>>
      extends AbstractKeys {

    private final UniqueKey<KeyRecordT> FIELD_INDEX_PKEY;

    private UniqueKeys(MetaFieldIndexTable<TableRefTypeT, KeyRecordT> fieldTable) {
      FIELD_INDEX_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.IDENTIFIER,
          fieldTable.POSITION);
    }
  }
}
