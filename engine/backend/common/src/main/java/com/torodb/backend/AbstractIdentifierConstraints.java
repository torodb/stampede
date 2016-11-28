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

package com.torodb.backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.metainf.FieldType;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

@Singleton
public abstract class AbstractIdentifierConstraints implements IdentifierConstraints {

  private static final char SEPARATOR = '_';
  private static final char ARRAY_DIMENSION_SEPARATOR = '$';

  private final ImmutableMap<FieldType, Character> fieldTypeIdentifiers;
  private final ImmutableMap<FieldType, String> scalarFieldTypeIdentifiers;
  private final ImmutableSet<String> restrictedSchemaNames;
  private final ImmutableSet<String> restrictedColumnNames;

  protected AbstractIdentifierConstraints(ImmutableSet<String> restrictedSchemaNames,
      ImmutableSet<String> restrictedColumnNames) {
    this.fieldTypeIdentifiers = Maps.immutableEnumMap(ImmutableMap.<FieldType, Character>builder()
        .put(FieldType.BINARY, 'r') // [r]aw bytes
        .put(FieldType.BOOLEAN, 'b') // [b]oolean
        .put(FieldType.DOUBLE, 'd') // [d]ouble
        .put(FieldType.INSTANT, 't') // [t]imestamp
        .put(FieldType.INTEGER, 'i') // [i]nteger
        .put(FieldType.LONG, 'l') // [l]ong
        .put(FieldType.NULL, 'n') // [n]ull
        .put(FieldType.STRING, 's') // [s]tring
        .put(FieldType.CHILD, 'e') // child [e]lement

        // Mongo types
        .put(FieldType.MONGO_OBJECT_ID, 'x')
        .put(FieldType.MONGO_TIME_STAMP, 'y')
        // No-Mongo types
        .put(FieldType.DATE, 'c') // [c]alendar
        .put(FieldType.TIME, 'm') // ti[m]e

        .build());

    ImmutableMap.Builder<FieldType, String> scalarFieldTypeIdentifiersBuilder =
        ImmutableMap.<FieldType, String>builder();
    Set<Character> fieldTypeIdentifierSet = new HashSet<>();
    for (FieldType fieldType : FieldType.values()) {
      if (!this.fieldTypeIdentifiers.containsKey(fieldType)) {
        throw new SystemException("FieldType " + fieldType
            + " has not been mapped to an identifier.");
      }

      char identifier = this.fieldTypeIdentifiers.get(fieldType);

      if ((identifier < 'a' || identifier > 'z') && (identifier < '0' || identifier > '9')) {
        throw new SystemException("FieldType " + fieldType + " has an unallowed identifier "
            + identifier);
      }

      if (fieldTypeIdentifierSet.contains(identifier)) {
        throw new SystemException("FieldType " + fieldType + " identifier "
            + identifier + " was used by another FieldType.");
      }

      fieldTypeIdentifierSet.add(identifier);

      scalarFieldTypeIdentifiersBuilder.put(fieldType, DocPartTableFields.SCALAR.fieldName
          + SEPARATOR + identifier);
    }

    this.scalarFieldTypeIdentifiers = Maps.immutableEnumMap(scalarFieldTypeIdentifiersBuilder
        .build());

    this.restrictedSchemaNames = ImmutableSet.<String>builder()
        .add(TorodbSchema.IDENTIFIER)
        .addAll(restrictedSchemaNames)
        .build();

    this.restrictedColumnNames = ImmutableSet.<String>builder()
        .add(DocPartTableFields.DID.fieldName)
        .add(DocPartTableFields.RID.fieldName)
        .add(DocPartTableFields.PID.fieldName)
        .add(DocPartTableFields.SEQ.fieldName)
        .addAll(scalarFieldTypeIdentifiers.values())
        .addAll(restrictedColumnNames)
        .build();
  }

  @Override
  public char getSeparator() {
    return SEPARATOR;
  }

  @Override
  public char getArrayDimensionSeparator() {
    return ARRAY_DIMENSION_SEPARATOR;
  }

  @Override
  public boolean isAllowedSchemaIdentifier(@Nonnull String schemaName) {
    return !restrictedSchemaNames.contains(schemaName);
  }

  @Override
  public boolean isAllowedTableIdentifier(@Nonnull String columnName) {
    return true;
  }

  @Override
  public boolean isAllowedColumnIdentifier(@Nonnull String columnName) {
    return !restrictedColumnNames.contains(columnName);
  }

  @Override
  public boolean isAllowedIndexIdentifier(@Nonnull String indexName) {
    return true;
  }

  @Override
  public boolean isSameIdentifier(@Nonnull String leftIdentifier, @Nonnull String rightIdentifier) {
    return leftIdentifier.equals(rightIdentifier);
    //leftIdentifier.toLowerCase(Locale.US).equals(rightIdentifier.toLowerCase(Locale.US));
  }

  @Override
  public char getFieldTypeIdentifier(FieldType fieldType) {
    return fieldTypeIdentifiers.get(fieldType);
  }

  @Override
  public String getScalarIdentifier(FieldType fieldType) {
    return scalarFieldTypeIdentifiers.get(fieldType);
  }
}
