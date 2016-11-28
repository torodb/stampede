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

package com.torodb.core.transaction.metainf;

import com.torodb.core.TableRef;

import java.util.Optional;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ImmutableMetaIndexField implements MetaIndexField {

  private final int position;
  private final TableRef tableRef;
  private final String name;
  private final FieldIndexOrdering ordering;

  public ImmutableMetaIndexField(int position, TableRef tableRef, String name,
      FieldIndexOrdering ordering) {
    super();
    this.position = position;
    this.tableRef = tableRef;
    this.name = name;
    this.ordering = ordering;
  }

  @Override
  public int getPosition() {
    return position;
  }

  @Override
  public TableRef getTableRef() {
    return tableRef;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public FieldIndexOrdering getOrdering() {
    return ordering;
  }

  @Override
  public String toString() {
    return defautToString();
  }

  @Override
  public boolean isCompatible(MetaDocPart docPart) {
    if (docPart.getTableRef().equals(tableRef)) {
      Optional<? extends MetaField> field = docPart.streamMetaFieldByName(name).findAny();

      if (field.isPresent()) {
        return true;
      }
    }

    if (!tableRef.isRoot()) {
      return name.equals(docPart.getTableRef().getName()) && tableRef.getParent().get().equals(
          tableRef);
    }

    return false;
  }

  @Override
  public boolean isCompatible(MetaDocPart docPart, MetaDocPartIndexColumn indexColumn) {
    if (ordering.equals(indexColumn.getOrdering())) {
      if (docPart.getTableRef().equals(tableRef)) {
        MetaField field = docPart.getMetaFieldByIdentifier(indexColumn.getIdentifier());

        if (field != null) {
          return name.equals(field.getName());
        }
      }

      if (!tableRef.isRoot()) {
        return name.equals(docPart.getTableRef().getName()) && docPart.getScalar(indexColumn
            .getIdentifier()) != null && tableRef.getParent().get().equals(tableRef);
      }
    }

    return false;
  }

  @Override
  public boolean isMatch(
      MetaDocPart docPart,
      String identifier,
      MetaDocPartIndexColumn indexColumn) {
    if (identifier.equals(indexColumn.getIdentifier())
        && ordering.equals(indexColumn.getOrdering())) {
      if (docPart.getTableRef().equals(tableRef)) {
        MetaField field = docPart.getMetaFieldByIdentifier(indexColumn.getIdentifier());

        if (field != null) {
          return name.equals(field.getName());
        }
      }

      if (!tableRef.isRoot()) {
        return name.equals(docPart.getTableRef().getName()) && docPart.getScalar(indexColumn
            .getIdentifier()) != null && tableRef.getParent().get().equals(tableRef);
      }
    }

    return false;
  }

  @Override
  public boolean isMatch(MetaIndexField otherIndexField) {
    return otherIndexField.getTableRef().equals(getTableRef()) && otherIndexField.getName().equals(
        getName()) && otherIndexField.getOrdering() == getOrdering();
  }

}
