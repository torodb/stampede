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
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonWriter;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.core.subdocument.structure.StructureElementVisitor;
import com.torodb.torod.db.backends.meta.CollectionSchema;

/**
 *
 */
public class StructureConverter implements Serializable {

    private static final String TABLE_ID = "t";
    private static final String INDEX_ID = "i";
    private static final Pattern SCAPED_KEY_PATTERN = Pattern.compile("(_+" + TABLE_ID + "|_+" + INDEX_ID + ')');
    private static final long serialVersionUID = 1L;

    private final CollectionSchema colSchema;
    private boolean initialized = false;
    private static final ToJsonVisitor TO_JSON = new ToJsonVisitor();

    public StructureConverter(CollectionSchema colSchema) {
        this.colSchema = colSchema;
    }

    public DocStructure from(String textStructure) {
        if (!initialized) {
            throw new IllegalStateException("The system is not initialized");
        }
        JsonReader reader = Json.createReader(new StringReader(textStructure));

        return readDocStructure(reader.readObject());
    }

    public String to(DocStructure userObject) {
        if (!initialized) {
            throw new IllegalStateException("The system is not initialized");
        }

        StringWriter stringWriter = new StringWriter(100);
        JsonWriter writer = Json.createWriter(stringWriter);

        JsonObject jsonStructure = (JsonObject) userObject.accept(TO_JSON, colSchema);

        writer.write(jsonStructure);

        return stringWriter.toString();
    }

    public void initialize() {
        initialized = true;
    }

    private static boolean isObjectKey(String key) {
        return !key.equals(INDEX_ID) && !key.equals(TABLE_ID);
    }

    private static String toStorableKey(String key) {
        Matcher matcher = SCAPED_KEY_PATTERN.matcher(key);
        if (matcher.matches() || key.equals(INDEX_ID) || key.equals(TABLE_ID)) {
            return '_' + key;
        }
        return key;
    }

    private static String fromStorableKey(String key) {
        Matcher matcher = SCAPED_KEY_PATTERN.matcher(key);
        if (matcher.matches()) {
            return key.substring(1);
        }
        return key;
    }

    private static boolean isStorableIndex(String candidate) {
        try {
            fromStorableIndex(candidate);
        } catch (NumberFormatException ex) {
            return false;
        }
        return true;
    }

    private static String toStorableIndex(int index) {
        return Integer.toString(index);
    }

    private static int fromStorableIndex(String storedIndex) {
        return Integer.parseInt(storedIndex);
    }

    private StructureElement readElement(JsonObject json) {
        if (isDocStructure(json)) {
            return readDocStructure(json);
        }
        if (isArrayStructure(json)) {
            return readArrayStructure(json);
        }
        throw new IllegalArgumentException("The given json element hasn't been recognized as document or array "
                + "structure (" + json + ")");
    }

    private DocStructure readDocStructure(JsonObject json) {
        DocStructure.Builder builder = new DocStructure.Builder();

        int tableId = json.getInt(TABLE_ID);
        int indexId;
        if (!json.containsKey(INDEX_ID)) {
            indexId = 0;
        }
        else {
            indexId = json.getInt(INDEX_ID);
        }

        builder.setType(colSchema.getSubDocType(tableId));
        builder.setIndex(indexId);

        for (Map.Entry<String, JsonValue> entry : json.entrySet()) {
            if (!isObjectKey(entry.getKey())) {
                continue;
            }
            String key = fromStorableKey(entry.getKey());
            if (!(entry.getValue() instanceof JsonObject)) {
                throw new ToroImplementationException(
                        "Unexpected structure/substructure "+json
                );
            }
            
            StructureElement element = readElement((JsonObject) entry.getValue());
            builder.add(key, element);
        }

        return builder.built();
    }

    private ArrayStructure readArrayStructure(JsonObject json) {
        ArrayStructure.Builder builder = new ArrayStructure.Builder();

        for (Map.Entry<String, JsonValue> entry : json.entrySet()) {
            int index = fromStorableIndex(entry.getKey());
            if (!(entry.getValue() instanceof JsonObject)) {
                throw new IllegalArgumentException("It was expected an array or document structure as value of " + entry.getKey());
            }
            StructureElement element = readElement((JsonObject) entry.getValue());

            builder.add(index, element);
        }

        return builder.built();
    }

    private boolean isDocStructure(JsonObject json) {
        return json.containsKey(TABLE_ID);
    }

    private boolean isArrayStructure(JsonObject json) {
        if (json.containsKey(TABLE_ID)) {
            return false;
        }
        for (String key : json.keySet()) {
            if (!isStorableIndex(key)) {
                return false;
            }
        }
        return true;
    }

    private static class ToJsonVisitor implements StructureElementVisitor<JsonValue, CollectionSchema> {

        @Override
        public JsonValue visit(DocStructure structure, CollectionSchema colSchema) {

            int tableId = colSchema.getTypeId(structure.getType());
            
            JsonObjectBuilder objBuilder = Json.createObjectBuilder();

            objBuilder.add(TABLE_ID, tableId);
            
            int index = structure.getIndex();
            if (index != 0) {
                objBuilder.add(INDEX_ID, index);
            }

            int attSize = structure.getElements().size() + structure.getType().getAttributeKeys().size();
            List<String> attributes = Lists.newArrayListWithCapacity(attSize);
            attributes.addAll(structure.getElements().keySet());
            attributes.addAll(structure.getType().getAttributeKeys());

            //Array attributes are stored as structure elements and attribute keys, so we need to store the visited attributes to do not visit them twice
            HashSet<String> visitedAttributes = Sets.newHashSetWithExpectedSize(attributes.size());

            Collections.sort(attributes);
            for (String attKey : attributes) {
                boolean isNew = visitedAttributes.add(attKey);
                if (!isNew) {
                    continue;
                }

                String keyToStore = toStorableKey(attKey);

                if (structure.getElements().containsKey(attKey)) { //is an object or array value
                    StructureElement element = structure.getElements().get(attKey);
                    JsonValue value = element.accept(this, colSchema);

                    objBuilder.add(keyToStore, value);
                }
            }

            return objBuilder.build();
        }

        @Override
        public JsonValue visit(ArrayStructure structure, CollectionSchema colSchema) {
            JsonObjectBuilder objBuilder = Json.createObjectBuilder();

            for (Map.Entry<Integer, ? extends StructureElement> entry : structure.getElements().entrySet()) {
                String keyToStore = toStorableIndex(entry.getKey());
                objBuilder.add(keyToStore, entry.getValue().accept(this, colSchema));
            }

            return objBuilder.build();
        }
    }
}
