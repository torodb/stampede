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

import com.torodb.core.exceptions.InvalidDatabaseException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.TableImpl;

@SuppressFBWarnings(
    value = "HE_HASHCODE_NO_EQUALS",
    justification =
    "Equals comparation is done in TableImpl class, which compares schema, name and fields"
)
public abstract class SemanticTable<R extends Record> extends TableImpl<R> {

  private static final long serialVersionUID = 1;

  public SemanticTable(String name, Schema schema, Table<R> aliased, Field<?>[] parameters,
      String comment) {
    super(name, schema, aliased, parameters, comment);
  }

  public SemanticTable(String name, Schema schema, Table<R> aliased, Field<?>[] parameters) {
    super(name, schema, aliased, parameters);
  }

  public SemanticTable(String name, Schema schema, Table<R> aliased) {
    super(name, schema, aliased);
  }

  public SemanticTable(String name, Schema schema) {
    super(name, schema);
  }

  public SemanticTable(String name) {
    super(name);
  }

  public boolean checkSemanticallyEquals(Table<R> table) throws InvalidDatabaseException {
    if (!table.getName().equals(getName())) {
      throw new InvalidDatabaseException("It was expected that "
          + "the table " + table + " was the " + getName() + " table, "
          + "but they are not semantically equals");
    }
    if (table.getSchema() == null || !getSchema().getName().equals(table.getSchema().getName())) {
      throw new InvalidDatabaseException("It was expected that "
          + "the table " + table + " was the " + getName() + " table, "
          + "but they are not semantically equals");
    }
    if (table.fields().length != fields().length) {
      throw new InvalidDatabaseException("It was expected that "
          + "the table " + table + " was the " + getName() + " table, "
          + "but they are not semantically equals");
    }
    for (Field<?> field : fields()) {
      boolean fieldFound = false;
      for (Field<?> tableField : table.fields()) {
        if (field.getName().equals(tableField.getName())) {
          fieldFound = true;
          break;
        }
      }

      if (!fieldFound) {
        throw new InvalidDatabaseException("It was expected that "
            + "the table " + table + " was the " + getName() + " table, "
            + "but they are not semantically equals");
      }
    }
    return true;
  }

}
