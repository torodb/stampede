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

package com.torodb.mongodb.language.update;

import com.google.common.collect.Lists;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.ListKvArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 *
 */
class UpdatedToroDocumentArrayBuilder {

  private boolean built;
  private List<KvValue<?>> values;
  private final Map<Integer, UpdatedToroDocumentArrayBuilder> subArrayBuilders =
      new HashMap<>();
  private final Map<Integer, UpdatedToroDocumentBuilder> subObjectBuilders =
      new HashMap<>();

  private UpdatedToroDocumentArrayBuilder() {
    built = false;
    values = new ArrayList<>();
  }

  public UpdatedToroDocumentArrayBuilder(int expectedSize) {
    built = false;
    values = Lists.newArrayListWithExpectedSize(expectedSize);
  }

  public static UpdatedToroDocumentArrayBuilder from(KvArray original) {
    UpdatedToroDocumentArrayBuilder result = new UpdatedToroDocumentArrayBuilder();
    result.copy(original);

    return result;
  }

  public static UpdatedToroDocumentArrayBuilder create() {
    return new UpdatedToroDocumentArrayBuilder();
  }

  public boolean contains(@Nonnegative int key) {
    return key < values.size() && (isValue(key)
        || isArrayBuilder(key)
        || isObjectBuilder(key));
  }

  public boolean isValue(int index) {
    return values.get(index) != null;
  }

  @Nonnull
  public KvValue<?> getValue(int index) {
    KvValue<?> result = values.get(index);
    if (result == null) {
      throw new IllegalArgumentException(
          "There is no value associated to '" + index + "' key");
    }
    return result;
  }

  public boolean isArrayBuilder(int index) {
    return subArrayBuilders.containsKey(index);
  }

  @Nonnull
  public UpdatedToroDocumentArrayBuilder getArrayBuilder(int index) {
    UpdatedToroDocumentArrayBuilder result = subArrayBuilders.get(index);
    if (result == null) {
      throw new IllegalArgumentException(
          "There is no array builder associated to '" + index + "' key");
    }
    return result;
  }

  public boolean isObjectBuilder(int index) {
    return subObjectBuilders.containsKey(index);
  }

  @Nonnull
  public UpdatedToroDocumentBuilder getObjectBuilder(int index) {
    UpdatedToroDocumentBuilder result = subObjectBuilders.get(index);
    if (result == null) {
      throw new IllegalArgumentException(
          "There is no object builder associated to '" + index + "' key");
    }
    return result;
  }

  private void setElement(int index, KvValue<?> element) {
    prepareSize(index);
    values.set(index, element);
    subObjectBuilders.remove(index);
    subArrayBuilders.remove(index);
  }

  private void setArrayBuilder(int index, UpdatedToroDocumentArrayBuilder builder) {
    prepareSize(index);
    values.set(index, null);
    subObjectBuilders.remove(index);
    subArrayBuilders.put(index, builder);
  }

  private void setObjectBuilder(int index, UpdatedToroDocumentBuilder builder) {
    prepareSize(index);
    values.set(index, null);
    subObjectBuilders.put(index, builder);
    subArrayBuilders.remove(index);
  }

  public UpdatedToroDocumentArrayBuilder newArray(int index) {
    checkNewBuild();

    UpdatedToroDocumentArrayBuilder result = new UpdatedToroDocumentArrayBuilder();
    setArrayBuilder(index, result);

    return result;
  }

  public UpdatedToroDocumentBuilder newObject(int index) {
    checkNewBuild();

    UpdatedToroDocumentBuilder result = UpdatedToroDocumentBuilder.create();
    setObjectBuilder(index, result);

    return result;
  }

  public boolean unset(int index) {
    if (values.size() >= index) {
      return false;
    }
    setElement(index, KvNull.getInstance());
    return true;
  }

  public UpdatedToroDocumentArrayBuilder add(KvValue<?> newVal) {
    checkNewBuild();

    values.add(newVal);

    return this;
  }

  public UpdatedToroDocumentArrayBuilder setValue(int index, KvValue<?> newValue) {
    checkNewBuild();

    if (newValue instanceof KvDocument) {
      newObject(index).copy((KvDocument) newValue);
    } else if (newValue instanceof KvArray) {
      newArray(index).copy((KvArray) newValue);
    } else {
      setElement(index, newValue);
    }
    return this;
  }

  public KvArray build() {
    built = true;

    for (Map.Entry<Integer, UpdatedToroDocumentBuilder> objectBuilder
        : subObjectBuilders.entrySet()) {

      KvValue<?> oldValue =
          values.set(
              objectBuilder.getKey(),
              objectBuilder.getValue().buildRoot()
          );

      assert oldValue == null;
    }
    for (Map.Entry<Integer, UpdatedToroDocumentArrayBuilder> arrayBuilder
        : subArrayBuilders.entrySet()) {

      KvValue<?> oldValue =
          values.set(
              arrayBuilder.getKey(),
              arrayBuilder.getValue().build()
          );

      assert oldValue == null;
    }
    subObjectBuilders.clear();
    subArrayBuilders.clear();

    return new ListKvArray(values);
  }

  private KvType getType(int index) {
    KvValue<?> value = values.get(index);
    if (value != null) {
      return value.getType();
    }
    if (isObjectBuilder(index)) {
      return DocumentType.INSTANCE;
    } else if (isArrayBuilder(index)) {
      return getArrayBuilder(index).calculateElementType();
    } else {
      throw new IllegalStateException();
    }
  }

  private KvType calculateElementType() {
    KvType result = null;
    for (int i = 0; i < values.size(); i++) {
      KvType iestType = getType(i);
      if (result == null) {
        result = iestType;
      } else if (!result.equals(iestType)) {
        result = GenericType.INSTANCE;
        break;
      }
    }

    return result;
  }

  void copy(KvArray original) {
    values.clear();
    subArrayBuilders.clear();
    subObjectBuilders.clear();

    for (int i = 0; i < original.size(); i++) {
      KvValue<?> value = original.get(i);

      if (value instanceof KvArray) {
        UpdatedToroDocumentArrayBuilder childBuilder = newArray(i);
        childBuilder.copy((KvArray) value);
      } else if (value instanceof KvDocument) {
        UpdatedToroDocumentBuilder childBuilder = newObject(i);
        childBuilder.copy((KvDocument) value);
      } else {
        setValue(i, value);
      }
    }
  }

  private void checkNewBuild() {
    if (built) {
      values = new ArrayList<>();
      built = false;
    }
  }

  private void prepareSize(int minSize) {
    for (int i = values.size(); i <= minSize; i++) {
      values.add(KvNull.getInstance());
    }
  }
}
