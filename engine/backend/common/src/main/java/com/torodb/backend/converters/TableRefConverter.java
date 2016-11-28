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

package com.torodb.backend.converters;

import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;

public class TableRefConverter {

  private TableRefConverter() {
  }

  public static String[] toStringArray(TableRef tableRef) {
    List<String> tableRefArray = new ArrayList<>();

    while (!tableRef.isRoot()) {
      String name = escapeTableRef(tableRef);
      tableRefArray.add(0, name);
      tableRef = tableRef.getParent().get();
    }

    return tableRefArray.toArray(new String[tableRefArray.size()]);
  }

  public static TableRef fromStringArray(TableRefFactory tableRefFactory, String[] tableRefArray) {
    TableRef tableRef = tableRefFactory.createRoot();

    for (String tableRefName : tableRefArray) {
      tableRef = createChild(tableRefFactory, tableRef, tableRefName);
    }

    return tableRef;
  }

  public static JsonArray toJsonArray(TableRef tableRef) {
    JsonArrayBuilder tableRefJsonArrayBuilder = Json.createArrayBuilder();

    for (String tableRefName : toStringArray(tableRef)) {
      tableRefJsonArrayBuilder.add(tableRefName);
    }

    return tableRefJsonArrayBuilder.build();
  }

  public static TableRef fromJsonArray(TableRefFactory tableRefFactory,
      JsonArray tableRefJsonArray) {
    TableRef tableRef = tableRefFactory.createRoot();

    for (JsonValue tableRefNameValue : tableRefJsonArray) {
      String tableRefName = ((JsonString) tableRefNameValue).getString();
      tableRef = createChild(tableRefFactory, tableRef, tableRefName);
    }

    return tableRef;
  }

  private static String escapeTableRef(TableRef tableRef) {
    String name;
    if (tableRef.isInArray()) {
      name = "$" + tableRef.getArrayDimension();
    } else {
      name = tableRef.getName().replace("$", "\\$");
    }
    return name;
  }

  private static TableRef createChild(TableRefFactory tableRefFactory, TableRef tableRef,
      String tableRefName) {
    if (isArrayDimension(tableRefName)) {
      Integer dimension = Integer.valueOf(tableRefName.substring(1));
      tableRef = tableRefFactory.createChild(tableRef, dimension);
    } else {
      tableRef = tableRefFactory.createChild(tableRef,
          unescapeTableRefName(tableRefName).intern());
    }
    return tableRef;
  }

  private static String unescapeTableRefName(String tableRefName) {
    return tableRefName.replace("\\$", "$");
  }

  private static boolean isArrayDimension(String name) {
    if (name.charAt(0) == '$') {
      for (int index = 1; index < name.length(); index++) {
        char charAt = name.charAt(index);
        if (charAt >= '0' && charAt <= '9') {
          return true;
        }
      }
    }
    return false;
  }
}
