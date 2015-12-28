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

package com.torodb.torod.mongodb.translator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.NumericDocValue;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.update.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import javax.annotation.Nonnull;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 *
 */
public class UpdateActionTranslator {
    
    UpdateActionTranslator() {
    }

    public static UpdateAction translate(BsonDocument updateObject) {
        
        if (isReplaceUpdate(updateObject)) {
            return translateReplaceObject(updateObject);
        }
        else {
            return translateComposedUpdate(updateObject);
        }
        
    }
    
    private static boolean isUpdateOperator(String key) {
        return UpdateOperator.isSubQuery(key);
    }

    private static boolean isReplaceUpdate(BsonDocument updateObject) {
        for (String key : updateObject.keySet()) {
            if (isUpdateOperator(key)) {
                return false;
            }
        }
        return true;
    }

    private static UpdateAction translateReplaceObject(BsonDocument updateObject) {
        return new SetDocumentUpdateAction(
                MongoValueConverter.translateObject(updateObject)
        );
    }

    private static UpdateAction translateComposedUpdate(BsonDocument updateObject) {
        CompositeUpdateAction.Builder builder 
                = new CompositeUpdateAction.Builder();
        
        for (String key : updateObject.keySet()) {
            if (!UpdateOperator.isSubQuery(key)) {
                throw new UserToroException("Unknown modifier: "+key);
            }
            translateSingleOperation(
                    builder,
                    UpdateOperator.fromKey(key), 
                    updateObject.get(key)
            );
        }
        return builder.build();
    }

    private static void translateSingleOperation(
            CompositeUpdateAction.Builder builder, 
            UpdateOperator key, 
            BsonValue value
    ) {
        if (!value.isDocument()) {
            throw new UserToroException("Modifiers operate on fields but found "
                    + "a "+value+" instead.");
        }
        BsonDocument castedValue = value.asDocument();
        switch (key) {
            case INCREMENT: {
                translateIncrement(builder, castedValue);
                break;
            }
            case MOVE: {
                translateMove(builder, castedValue);
                break;
            }
            case MULTIPLY: {
                translateMultiply(builder, castedValue);
                break;
            }
            case SET_CURRENT_DATE: {
                translateSetCurrentDate(builder, castedValue);
                break;
            }
            case SET_FIELD: {
                translateSetField(builder, castedValue);
                break;
            }
            case UNSET_FIELD: {
                translateUnsetField(builder, castedValue);
                break;
            }
        }
    }
    
    private static AttributeReference parseAttributReferenceAsObjectReference(
            String key) {
        ImmutableList.Builder<AttributeReference.Key> newKeysBuilder 
                = ImmutableList.<AttributeReference.Key>builder();
        
        StringTokenizer st = new StringTokenizer(key, ".");
        while (st.hasMoreTokens()) {
            newKeysBuilder.add(new AttributeReference.ObjectKey(st.nextToken()));
        }
        return new AttributeReference(newKeysBuilder.build());
    }
    
    private static Collection<AttributeReference> parseAttributeReference(
            String key) {
        String[] tokens = key.split("\\.");
        
        List<AttributeReference.Key> keys = Lists.newArrayList();
        List<AttributeReference> attRefs = Lists.newArrayList();
        parseAttributeReference(tokens, 0, keys, attRefs);
        
        return attRefs;
    }
    
    private static void parseAttributeReference(
            String[] tokens,
            int depth,
            List<AttributeReference.Key> keys,
            Collection<AttributeReference> attRefs) {
        if (tokens.length == depth) {
            attRefs.add(new AttributeReference(keys));
        }
        else {
            String nextKey = tokens[depth];
            
            keys.add(new AttributeReference.ObjectKey(nextKey));
            parseAttributeReference(tokens, depth + 1, keys, attRefs);
            keys.remove(keys.size() - 1);
            try {
                int keyAsInt = Integer.parseInt(nextKey);
                
                keys.add(new AttributeReference.ArrayKey(keyAsInt));
                parseAttributeReference(tokens, depth + 1, keys, attRefs);
                keys.remove(keys.size() - 1);
            } catch (NumberFormatException ex) { //the key is not an array key candidate
            }
        }
    }
    
    private static void translateIncrement(
            CompositeUpdateAction.Builder builder, BsonDocument argument) {
        for (String key : argument.keySet()) {
            Collection<AttributeReference> attRefs 
                    = parseAttributeReference(key);
            DocValue translatedValue 
                    = MongoValueConverter.translateBSON(argument.get(key));
            
            if (!(translatedValue instanceof NumericDocValue)) {
                throw new UserToroException("Cannot increment with a "
                        + "non-numeric argument");
            }
            builder.add(
                    new IncrementUpdateAction(
                            attRefs,
                            (NumericDocValue) translatedValue
                    )
            );
        }
    }

    private static void translateMove(
            CompositeUpdateAction.Builder builder, BsonDocument argument) {
        for (String key : argument.keySet()) {
            Collection<AttributeReference> attRefs
                    = parseAttributeReference(key);
            if (!argument.get(key).isString()) {
                throw new UserToroException("The 'to' field for $rename must "
                        + "be a string, but "+argument.get(key)+" were found "
                        + "with key "+key);
            }
            AttributeReference newRef
                    = parseAttributReferenceAsObjectReference(
                            argument.get(key).asString().getValue()
                    );
            
            builder.add(
                    new MoveUpdateAction(
                            attRefs,
                            newRef
                    )
            );
        }
    }

    private static void translateMultiply(
            CompositeUpdateAction.Builder builder, BsonDocument argument) {
        for (String key : argument.keySet()) {
            Collection<AttributeReference> attRefs
                    = parseAttributeReference(key);
            DocValue translatedValue 
                    = MongoValueConverter.translateBSON(argument.get(key));
            
            if (!(translatedValue instanceof NumericDocValue)) {
                throw new UserToroException("Cannot multiply with a "
                        + "non-numeric argument");
            }
            builder.add(
                    new MultiplyUpdateAction(
                            attRefs,
                            (NumericDocValue) translatedValue
                    )
            );
        }
    }

    private static void translateSetCurrentDate(
            CompositeUpdateAction.Builder builder, BsonDocument argument) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private static void translateSetField(
            CompositeUpdateAction.Builder builder, BsonDocument argument) {
        for (String key : argument.keySet()) {
            Collection<AttributeReference> attRefs
                    = parseAttributeReference(key);
            DocValue translatedValue 
                    = MongoValueConverter.translateBSON(argument.get(key));
            
            builder.add(
                    new SetFieldUpdateAction(
                            attRefs,
                            translatedValue
                    )
            );
        }
    }

    private static void translateUnsetField(
            CompositeUpdateAction.Builder builder, BsonDocument argument) {
        for (String key : argument.keySet()) {
            Collection<AttributeReference> attRefs
                    = parseAttributeReference(key);
            
            builder.add(
                    new UnsetFieldUpdateAction(attRefs)
            );
        }
    }

    private static enum UpdateOperator {

        INCREMENT("$inc"),
        MOVE("$rename"),
        MULTIPLY("$mult"),
        SET_CURRENT_DATE("$currentDate"),
        SET_FIELD("$set"),
        UNSET_FIELD("$unset");
        
        private static final Map<String, UpdateOperator> operandsByKey;
        private final String key;
        static {
            operandsByKey = Maps.newHashMapWithExpectedSize(UpdateOperator.values().length);
            for (UpdateOperator operand : UpdateOperator.values()) {
                operandsByKey.put(operand.key, operand);
            }
        }
        
        private UpdateOperator(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
        
        public static boolean isSubQuery(String key) {
            return operandsByKey.containsKey(key);
        }

        @Nonnull
        public static UpdateOperator fromKey(String key) {
            UpdateOperator result = operandsByKey.get(key);
            if (result == null) {
                throw new IllegalArgumentException("There is no operand whose "
                        + "key is '" + key + "'");
            }
            return result;
        }
    }
}
