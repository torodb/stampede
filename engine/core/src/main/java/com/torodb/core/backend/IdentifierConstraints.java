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

package com.torodb.core.backend;

import com.torodb.core.transaction.metainf.FieldType;

import javax.annotation.Nonnull;

public interface IdentifierConstraints {

  int identifierMaxSize();

  boolean isAllowedSchemaIdentifier(@Nonnull String identifier);

  boolean isAllowedTableIdentifier(@Nonnull String identifier);

  boolean isAllowedColumnIdentifier(@Nonnull String identifier);

  boolean isAllowedIndexIdentifier(@Nonnull String identifier);

  boolean isSameIdentifier(@Nonnull String leftIdentifier, @Nonnull String rightIdentifier);

  char getSeparator();

  char getArrayDimensionSeparator();

  char getFieldTypeIdentifier(FieldType fieldType);

  String getScalarIdentifier(FieldType fieldType);
}
