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

import com.google.common.collect.ImmutableList;
import com.torodb.backend.InternalField;
import com.torodb.backend.InternalField.DidInternalField;
import com.torodb.backend.InternalField.PidInternalField;
import com.torodb.backend.InternalField.RidInternalField;
import com.torodb.backend.InternalField.SeqInternalField;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.MetaDocPartRecord;
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
public abstract class MetaDocPartTable<TableRefTypeT, R extends MetaDocPartRecord<TableRefTypeT>>
    extends SemanticTable<R> {

  private static final long serialVersionUID = 1664366669485866827L;

  public static final String TABLE_NAME = "doc_part";

  public enum TableFields {
    DATABASE("database"),
    COLLECTION("collection"),
    TABLE_REF("table_ref"),
    IDENTIFIER("identifier"),
    LAST_RID("last_rid");

    public final String fieldName;

    TableFields(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String toString() {
      return fieldName;
    }
  }

  public enum DocPartTableFields {
    DID("did"),
    RID("rid"),
    PID("pid"),
    SEQ("seq"),
    SCALAR("v"),;

    public final String fieldName;

    DocPartTableFields(String fieldName) {
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
   * The column <code>torodb.container.database</code>.
   */
  public final TableField<R, String> DATABASE =
      createDatabaseField();

  /**
   * The column <code>torodb.container.collection</code>.
   */
  public final TableField<R, String> COLLECTION =
      createCollectionField();

  /**
   * The column <code>torodb.container.path</code>.
   */
  public final TableField<R, TableRefTypeT> TABLE_REF =
      createTableRefField();

  /**
   * The column <code>torodb.container.table_name</code>.
   */
  public final TableField<R, String> IDENTIFIER =
      createIdentifierField();

  /**
   * The column <code>torodb.container.last_rid</code>.
   */
  public final TableField<R, Integer> LAST_RID =
      createLastRidField();

  public final InternalField<Integer> DID =
      new DidInternalField(createDidField());
  public final InternalField<Integer> RID =
      new RidInternalField(createRidField());
  public final InternalField<Integer> PID =
      new PidInternalField(createPidField());
  public final InternalField<Integer> SEQ =
      new SeqInternalField(createSeqField());

  public final ImmutableList<InternalField<?>> ROOT_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(DID)
      .build();
  public final ImmutableList<InternalField<?>> FIRST_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(DID)
      .add(RID)
      .add(SEQ)
      .build();
  public final ImmutableList<InternalField<?>> FIELDS = ImmutableList.<InternalField<?>>builder()
      .add(DID)
      .add(RID)
      .add(PID)
      .add(SEQ)
      .build();

  public final ImmutableList<InternalField<?>> PRIMARY_KEY_ROOT_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(DID)
      .build();
  public final ImmutableList<InternalField<?>> PRIMARY_KEY_FIRST_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(RID)
      .build();
  public final ImmutableList<InternalField<?>> PRIMARY_KEY_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(RID)
      .build();

  public final ImmutableList<InternalField<?>> REFERENCE_FIRST_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(DID)
      .build();
  public final ImmutableList<InternalField<?>> REFERENCE_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(PID)
      .build();

  public final ImmutableList<InternalField<?>> FOREIGN_ROOT_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(DID)
      .build();
  public final ImmutableList<InternalField<?>> FOREIGN_FIRST_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(RID)
      .build();
  public final ImmutableList<InternalField<?>> FOREIGN_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(RID)
      .build();

  public final ImmutableList<InternalField<?>> READ_ROOT_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(DID)
      .build();
  public final ImmutableList<InternalField<?>> READ_FIRST_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(DID)
      .add(SEQ)
      .build();
  public final ImmutableList<InternalField<?>> READ_FIELDS = ImmutableList
      .<InternalField<?>>builder()
      .add(DID)
      .add(PID)
      .add(SEQ)
      .build();

  protected abstract TableField<R, String> createDatabaseField();

  protected abstract TableField<R, String> createCollectionField();

  protected abstract TableField<R, TableRefTypeT> createTableRefField();

  protected abstract TableField<R, String> createIdentifierField();

  protected abstract TableField<R, Integer> createLastRidField();

  protected abstract Field<Integer> createDidField();

  protected abstract Field<Integer> createRidField();

  protected abstract Field<Integer> createPidField();

  protected abstract Field<Integer> createSeqField();

  private final UniqueKeys<TableRefTypeT, R> uniqueKeys;

  /**
   * Create a <code>torodb.doc_part</code> table reference
   */
  public MetaDocPartTable() {
    this(TABLE_NAME, null);
  }

  protected MetaDocPartTable(String alias, Table<R> aliased) {
    this(alias, aliased, null);
  }

  protected MetaDocPartTable(String alias, Table<R> aliased, Field<?>[] parameters) {
    super(alias, TorodbSchema.TORODB, aliased, parameters, "");

    this.uniqueKeys = new UniqueKeys<TableRefTypeT, R>(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UniqueKey<R> getPrimaryKey() {
    return uniqueKeys.CONTAINER_PKEY;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<UniqueKey<R>> getKeys() {
    return Arrays.<UniqueKey<R>>asList(uniqueKeys.CONTAINER_PKEY,
        uniqueKeys.CONTAINER_TABLE_NAME_UNIQUE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract MetaDocPartTable<TableRefTypeT, R> as(String alias);

  /**
   * Rename this table
   */
  public abstract MetaDocPartTable<TableRefTypeT, R> rename(String name);

  public UniqueKeys<TableRefTypeT, R> getUniqueKeys() {
    return uniqueKeys;
  }

  @SuppressWarnings({"checkstyle:LineLength", "checkstyle:AbbreviationAsWordInName",
      "checkstyle:MemberName"})
  public static class UniqueKeys<TableRefTypeT, KeyRecordT extends MetaDocPartRecord<TableRefTypeT>>
      extends AbstractKeys {

    private final UniqueKey<KeyRecordT> CONTAINER_PKEY;
    private final UniqueKey<KeyRecordT> CONTAINER_TABLE_NAME_UNIQUE;

    private UniqueKeys(MetaDocPartTable<TableRefTypeT, KeyRecordT> containerTable) {
      CONTAINER_PKEY = createUniqueKey(containerTable, containerTable.DATABASE,
          containerTable.COLLECTION, containerTable.TABLE_REF);
      CONTAINER_TABLE_NAME_UNIQUE = createUniqueKey(containerTable, containerTable.DATABASE,
          containerTable.IDENTIFIER);
    }
  }
}
