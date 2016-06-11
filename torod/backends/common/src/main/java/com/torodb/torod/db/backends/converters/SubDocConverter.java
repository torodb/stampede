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

package com.torodb.torod.db.backends.converters;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Map;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;

import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.tables.SubDocHelper;
import com.torodb.torod.db.backends.tables.SubDocTable;

/**
 *
 */
@Singleton
public class SubDocConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String ID_COLUMN_NAME = "id";

    private final DatabaseInterface databaseInterface;

    @Inject
    public SubDocConverter(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    public SubDocument from(String subdocAsJson, SubDocType subDocType) {
        JsonReader reader = Json.createReader(new StringReader(subdocAsJson));

        JsonObject jsonObject = reader.readObject();
        
        SubDocument.Builder builder = SubDocument.Builder.withKnownType(subDocType);

        for (Map.Entry<String, JsonValue> entry : jsonObject.entrySet()) {
            Object objectValue = jsonValueToObject(entry.getValue());
            
            convertAttribute(builder, entry.getKey(), objectValue, subDocType);
        }
        
        return builder.build();
    }

    /**
     *
     * @param field
     * @return the name of the subdoc attribute represented by the given field or null if the field doesn't correspond
     *         with an attribute
     */
    @Nullable
    private static String getAttributeName(String fieldName) {
        if (fieldName.equals(SubDocTable.DID_COLUMN_NAME)
                || fieldName.equals(SubDocTable.INDEX_COLUMN_NAME)) {
            return null;
        }

        return SubDocHelper.toAttributeName(fieldName);
    }

    private void convertAttribute(
            SubDocument.Builder builder, 
            String columnName, 
            Object jsonValue,
            SubDocType subDocType
    ) {
        String attName = getAttributeName(columnName);
        if (attName != null) { //it is an attribute
            SubDocAttribute attribute = subDocType.getAttribute(attName);
            
            ScalarValue<?> subDocValue = databaseInterface.getValueToJsonConverterProvider()
                    .getConverter(attribute.getType())
                    .toValue(jsonValue);
            builder.add(attribute, subDocValue);
        } else { //it is a special field
            if (columnName.equals(SubDocTable.DID_COLUMN_NAME)) {
                builder.setDocumentId((Integer) jsonValue);
            }
            else if (columnName.equals(SubDocTable.INDEX_COLUMN_NAME)) {
                Integer index;
                if (jsonValue == null) {
                    index = 0;
                }
                else {
                    index = (Integer) jsonValue;
                }
                builder.setIndex(index);
            }
            else {
                throw new IllegalArgumentException("The given record contains the attribute " + columnName + " which is not recognized");
            }
        }
    }
    
    private Object jsonValueToObject(JsonValue jsonValue) {
        switch (jsonValue.getValueType()) {
            case FALSE:
                return Boolean.FALSE;
            case TRUE:
                return Boolean.TRUE;
            case NULL:
                return null;
            case STRING: {
                JsonString casted = (JsonString) jsonValue;
                return casted.getString();
            }
            case NUMBER: {
                JsonNumber number = (JsonNumber) jsonValue;
                if (number.isIntegral()) {
                    try {
                        long l = number.longValueExact();
                        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                            return l;
                        }
                        return number.intValueExact();
                    } catch (ArithmeticException ex) {
                        return number.bigIntegerValueExact();
                    }
                }
                return number.doubleValue();
            }
            case ARRAY: {
                assert jsonValue instanceof JsonArray;
                return (JsonArray) jsonValue;
            }
            case OBJECT:
            default:
                throw new AssertionError(jsonValue + " is not accepted as a column value");
        }
    }
}
