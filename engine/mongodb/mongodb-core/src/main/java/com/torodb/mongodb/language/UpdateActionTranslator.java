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

package com.torodb.mongodb.language;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.torodb.core.exceptions.user.UpdateException;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvNumeric;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.language.update.CompositeUpdateAction;
import com.torodb.mongodb.language.update.IncrementUpdateAction;
import com.torodb.mongodb.language.update.MoveUpdateAction;
import com.torodb.mongodb.language.update.MultiplyUpdateAction;
import com.torodb.mongodb.language.update.SetDocumentUpdateAction;
import com.torodb.mongodb.language.update.SetFieldUpdateAction;
import com.torodb.mongodb.language.update.UnsetFieldUpdateAction;
import com.torodb.mongodb.language.update.UpdateAction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.annotation.Nonnull;

/**
 *
 */
public class UpdateActionTranslator {

  UpdateActionTranslator() {
  }

  public static UpdateAction translate(BsonDocument updateObject) throws UpdateException {

    if (isReplaceUpdate(updateObject)) {
      return translateReplaceObject(updateObject);
    } else {
      return translateComposedUpdate(updateObject);
    }

  }

  private static boolean isUpdateOperator(String key) {
    return UpdateOperator.isSubQuery(key);
  }

  private static boolean isReplaceUpdate(BsonDocument updateObject) {
    for (Entry<?> entry : updateObject) {
      if (isUpdateOperator(entry.getKey())) {
        return false;
      }
    }
    return true;
  }

  private static UpdateAction translateReplaceObject(BsonDocument updateObject) {
    return new SetDocumentUpdateAction(
        (KvDocument) MongoWpConverter.translate(updateObject));
  }

  private static UpdateAction translateComposedUpdate(BsonDocument updateObject) throws
      UpdateException {
    CompositeUpdateAction.Builder builder =
        new CompositeUpdateAction.Builder();

    for (Entry<?> entry : updateObject) {
      String key = entry.getKey();
      if (!UpdateOperator.isSubQuery(key)) {
        throw new UpdateException("Unknown modifier: " + key);
      }
      translateSingleOperation(
          builder,
          UpdateOperator.fromKey(key),
          entry.getValue()
      );
    }
    return builder.build();
  }

  private static void translateSingleOperation(
      CompositeUpdateAction.Builder builder,
      UpdateOperator key,
      BsonValue<?> value
  ) throws UpdateException {
    if (!value.isDocument()) {
      throw new UpdateException("Modifiers operate on fields but found "
          + "a " + value + " instead.");
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
      default:
    }
  }

  private static AttributeReference parseAttributReferenceAsObjectReference(
      String key) {
    ImmutableList.Builder<AttributeReference.Key<?>> newKeysBuilder =
        ImmutableList.<AttributeReference.Key<?>>builder();

    StringTokenizer st = new StringTokenizer(key, ".");
    while (st.hasMoreTokens()) {
      newKeysBuilder.add(new AttributeReference.ObjectKey(st.nextToken()));
    }
    return new AttributeReference(newKeysBuilder.build());
  }

  private static Collection<AttributeReference> parseAttributeReference(
      String key) {
    String[] tokens = key.split("\\.");

    List<AttributeReference.Key<?>> keys = Lists.newArrayList();
    List<AttributeReference> attRefs = Lists.newArrayList();
    parseAttributeReference(tokens, 0, keys, attRefs);

    return attRefs;
  }

  private static void parseAttributeReference(
      String[] tokens,
      int depth,
      List<AttributeReference.Key<?>> keys,
      Collection<AttributeReference> attRefs) {
    if (tokens.length == depth) {
      attRefs.add(new AttributeReference(keys));
    } else {
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
      CompositeUpdateAction.Builder builder, BsonDocument argument) throws UpdateException {
    for (Entry<?> entry : argument) {
      Collection<AttributeReference> attRefs =
          parseAttributeReference(entry.getKey());
      KvValue<?> translatedValue = MongoWpConverter.translate(entry.getValue());

      if (!(translatedValue instanceof KvNumeric)) {
        throw new UpdateException("Cannot increment with a "
            + "non-numeric argument");
      }
      builder.add(new IncrementUpdateAction(
          attRefs,
          (KvNumeric<?>) translatedValue
      ), false
      );
    }
  }

  private static void translateMove(
      CompositeUpdateAction.Builder builder, BsonDocument argument) throws UpdateException {
    for (Entry<?> entry : argument) {
      Collection<AttributeReference> attRefs =
          parseAttributeReference(entry.getKey());
      if (!entry.getValue().isString()) {
        throw new UpdateException("The 'to' field for $rename must "
            + "be a string, but " + entry.getValue() + " were found "
            + "with key " + entry.getKey());
      }
      AttributeReference newRef =
          parseAttributReferenceAsObjectReference(
              entry.getValue().asString().getValue()
          );

      builder.add(
          new MoveUpdateAction(
              attRefs,
              newRef
          ), false
      );
    }
  }

  private static void translateMultiply(
      CompositeUpdateAction.Builder builder, BsonDocument argument) throws UpdateException {
    for (Entry<?> entry : argument) {
      Collection<AttributeReference> attRefs =
          parseAttributeReference(entry.getKey());
      KvValue<?> translatedValue =
          MongoWpConverter.translate(entry.getValue());

      if (!(translatedValue instanceof KvNumeric)) {
        throw new UpdateException("Cannot multiply with a "
            + "non-numeric argument");
      }
      builder.add(new MultiplyUpdateAction(
          attRefs,
          (KvNumeric<?>) translatedValue
      ), false
      );
    }
  }

  private static void translateSetCurrentDate(
      CompositeUpdateAction.Builder builder, BsonDocument argument) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private static void translateSetField(
      CompositeUpdateAction.Builder builder, BsonDocument argument) {
    for (Entry<?> entry : argument) {
      Collection<AttributeReference> attRefs =
          parseAttributeReference(entry.getKey());
      KvValue<?> translatedValue =
          MongoWpConverter.translate(entry.getValue());

      builder.add(
          new SetFieldUpdateAction(
              attRefs,
              translatedValue
          ), false
      );
    }
  }

  private static void translateUnsetField(
      CompositeUpdateAction.Builder builder, BsonDocument argument) {
    for (Entry<?> entry : argument) {
      Collection<AttributeReference> attRefs =
          parseAttributeReference(entry.getKey());

      builder.add(new UnsetFieldUpdateAction(attRefs), false);
    }
  }

  private static enum UpdateOperator {

    INCREMENT("$inc"),
    MOVE("$rename"),
    MULTIPLY("$mul"),
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
