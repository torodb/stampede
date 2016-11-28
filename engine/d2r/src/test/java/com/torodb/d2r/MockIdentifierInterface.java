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

package com.torodb.d2r;

import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.transaction.metainf.FieldType;

public class MockIdentifierInterface implements IdentifierConstraints {

  @Override
  public int identifierMaxSize() {
    return 128;
  }

  @Override
  public boolean isAllowedSchemaIdentifier(String identifier) {
    return !identifier.equals("unallowed_schema");
  }

  @Override
  public boolean isAllowedTableIdentifier(String identifier) {
    return !identifier.equals("unallowed_table");
  }

  @Override
  public boolean isAllowedColumnIdentifier(String identifier) {
    return !identifier.equals("unallowed_column_s");
  }

  @Override
  public boolean isAllowedIndexIdentifier(String identifier) {
    return !identifier.equals("unallowed_index");
  }

  @Override
  public boolean isSameIdentifier(String leftIdentifier, String rightIdentifier) {
    return leftIdentifier.equals(rightIdentifier);
  }

  @Override
  public char getSeparator() {
    return '_';
  }

  @Override
  public char getArrayDimensionSeparator() {
    return '$';
  }

  private static final char[] FIELD_TYPE_IDENTIFIERS = new char[FieldType.values().length];

  static {
    FIELD_TYPE_IDENTIFIERS[FieldType.BINARY.ordinal()] = 'r'; // [r]aw
    FIELD_TYPE_IDENTIFIERS[FieldType.BOOLEAN.ordinal()] = 'b'; // [b]inary
    FIELD_TYPE_IDENTIFIERS[FieldType.DATE.ordinal()] = 'c'; // [c]alendar
    FIELD_TYPE_IDENTIFIERS[FieldType.DOUBLE.ordinal()] = 'd'; // [d]ouble
    FIELD_TYPE_IDENTIFIERS[FieldType.INSTANT.ordinal()] = 'g'; // [G]eorge Gamow or Admiral [G]race Hopper that were the earliest users of the term nanosecond
    FIELD_TYPE_IDENTIFIERS[FieldType.INTEGER.ordinal()] = 'i'; // [i]nteger
    FIELD_TYPE_IDENTIFIERS[FieldType.LONG.ordinal()] = 'l'; // [l]ong
    FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_OBJECT_ID.ordinal()] = 'x';
    FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_TIME_STAMP.ordinal()] = 'y';
    FIELD_TYPE_IDENTIFIERS[FieldType.NULL.ordinal()] = 'n'; // [n]ull
    FIELD_TYPE_IDENTIFIERS[FieldType.STRING.ordinal()] = 's'; // [s]tring
    FIELD_TYPE_IDENTIFIERS[FieldType.TIME.ordinal()] = 't'; // [t]ime
    FIELD_TYPE_IDENTIFIERS[FieldType.CHILD.ordinal()] = 'e'; // [e]lement
  }

  @Override
  public char getFieldTypeIdentifier(FieldType fieldType) {
    return FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()];
  }

  @Override
  public String getScalarIdentifier(FieldType fieldType) {
    return "v_" + FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()];
  }

}
