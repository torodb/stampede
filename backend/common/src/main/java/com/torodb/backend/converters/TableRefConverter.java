/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.backend.converters;

import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;

import com.torodb.core.TableRef;
import com.torodb.core.impl.TableRefImpl;

public class TableRefConverter {
    public static String[] toStringArray(TableRef tableRef) {
        List<String> tableRefArray = new ArrayList<>();
        
        while(!tableRef.isRoot()) {
            tableRefArray.add(0, tableRef.getName());
            tableRef = tableRef.getParent().get();
        }
        
        return tableRefArray.toArray(new String[tableRefArray.size()]);
    }
    
    public static TableRef fromStringArray(String[] tableRefArray) {
        TableRef tableRef = TableRefImpl.createRoot();
        
        for (String tableRefName : tableRefArray) {
            tableRef = TableRefImpl.createChild(tableRef, tableRefName.intern());
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
    
    public static TableRef fromJsonArray(JsonArray tableRefJsonArray) {
        TableRef tableRef = TableRefImpl.createRoot();
        
        for (JsonValue tableRefName : tableRefJsonArray) {
            tableRef = TableRefImpl.createChild(tableRef, 
                    ((JsonString) tableRefName).getString().intern());
        }
        
        return tableRef;
    }
}
