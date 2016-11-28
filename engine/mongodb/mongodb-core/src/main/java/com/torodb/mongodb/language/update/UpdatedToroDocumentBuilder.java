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

import com.torodb.core.document.ToroDocument;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvDocument.DocEntry;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.MapKvDocument;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

/**
 *
 */
public class UpdatedToroDocumentBuilder {

  private LinkedHashMap<String, KvValue<?>> values = new LinkedHashMap<>();
  private final Map<String, UpdatedToroDocumentArrayBuilder> subArrayBuilders = new HashMap<>();
  private final Map<String, UpdatedToroDocumentBuilder> subObjectBuilders = new HashMap<>();
  private boolean built = false;
  private boolean updated = false;

  private UpdatedToroDocumentBuilder() {
  }

  public void clear() {
    if (built) {
      built = false;
      updated = false;
      values = new LinkedHashMap<>();
    } else {
      values.clear();
    }
    subArrayBuilders.clear();
    subObjectBuilders.clear();
  }

  public static UpdatedToroDocumentBuilder create() {
    return new UpdatedToroDocumentBuilder();
  }

  public static UpdatedToroDocumentBuilder from(ToroDocument original) {
    UpdatedToroDocumentBuilder result = UpdatedToroDocumentBuilder.create();
    result.copy(original);
    return result;
  }

  public static UpdatedToroDocumentBuilder from(KvDocument original) {
    UpdatedToroDocumentBuilder result = UpdatedToroDocumentBuilder.create();
    result.copy(original);
    return result;
  }

  public boolean contains(String key) {
    return isValue(key)
        || isArrayBuilder(key)
        || isObjectBuilder(key);
  }

  public boolean isValue(String key) {
    return values.containsKey(key);
  }

  @Nonnull
  public KvValue<?> getValue(String key) {
    KvValue<?> result = values.get(key);
    if (result == null) {
      throw new IllegalArgumentException(
          "There is no value associated to '" + key + "' key");
    }
    return result;
  }

  public boolean isArrayBuilder(String key) {
    return subArrayBuilders.containsKey(key);
  }

  @Nonnull
  public UpdatedToroDocumentArrayBuilder getArrayBuilder(String key) {
    UpdatedToroDocumentArrayBuilder result = subArrayBuilders.get(key);
    if (result == null) {
      throw new IllegalArgumentException(
          "There is no array builder associated to '" + key + "' key");
    }
    return result;
  }

  public boolean isObjectBuilder(String key) {
    return subObjectBuilders.containsKey(key);
  }

  @Nonnull
  public UpdatedToroDocumentBuilder getObjectBuilder(String key) {
    UpdatedToroDocumentBuilder result = subObjectBuilders.get(key);
    if (result == null) {
      throw new IllegalArgumentException(
          "There is no object builder associated to '" + key + "' key");
    }
    return result;
  }

  public UpdatedToroDocumentBuilder putValue(String key, KvValue<?> value) {
    checkNewBuild();

    if (value instanceof KvDocument) {
      newObject(key).copy((KvDocument) value);
    } else if (value instanceof KvArray) {
      newArray(key).copy((KvArray) value);
    } else {
      values.put(key, value);
      subArrayBuilders.remove(key);
      subObjectBuilders.remove(key);
    }

    return this;
  }

  public UpdatedToroDocumentArrayBuilder newArray(String key) {
    checkNewBuild();

    UpdatedToroDocumentArrayBuilder result = UpdatedToroDocumentArrayBuilder.create();

    values.remove(key);
    subArrayBuilders.put(key, result);
    subObjectBuilders.remove(key);

    return result;
  }

  public UpdatedToroDocumentBuilder newObject(String key) {
    checkNewBuild();

    UpdatedToroDocumentBuilder result = UpdatedToroDocumentBuilder.create();

    values.remove(key);
    subArrayBuilders.remove(key);
    subObjectBuilders.put(key, result);

    return result;
  }

  public boolean unset(String key) {
    boolean result = false;
    result |= values.remove(key) != null;
    result |= subArrayBuilders.remove(key) != null;
    result |= subObjectBuilders.remove(key) != null;

    return result;
  }

  public KvDocument buildRoot() {
    built = true;

    for (Entry<String, UpdatedToroDocumentBuilder> objectBuilder
        : subObjectBuilders.entrySet()) {

      KvValue<?> oldValue =
          values.put(
              objectBuilder.getKey(),
              objectBuilder.getValue().buildRoot()
          );

      assert oldValue == null;
    }
    for (Entry<String, UpdatedToroDocumentArrayBuilder> arrayBuilder
        : subArrayBuilders.entrySet()) {

      KvValue<?> oldValue =
          values.put(
              arrayBuilder.getKey(),
              arrayBuilder.getValue().build()
          );

      assert oldValue == null;
    }
    subObjectBuilders.clear();
    subArrayBuilders.clear();

    return new MapKvDocument(values);
  }

  void copy(ToroDocument original) {
    copy(original.getRoot());
  }

  void copy(KvDocument original) {
    values.clear();
    subArrayBuilders.clear();
    subObjectBuilders.clear();

    for (DocEntry<?> entry : original) {
      KvValue<?> value = entry.getValue();
      if (value instanceof KvArray) {
        UpdatedToroDocumentArrayBuilder childBuilder = newArray(entry.getKey());
        childBuilder.copy((KvArray) value);
      } else if (value instanceof KvDocument) {
        UpdatedToroDocumentBuilder childBuilder = newObject(entry.getKey());
        childBuilder.copy((KvDocument) value);
      } else {
        putValue(entry.getKey(), value);
      }
    }
  }

  private void checkNewBuild() {
    if (built) {
      values = new LinkedHashMap<>();
      built = false;
    }
  }

  public UpdatedToroDocumentBuilder setUpdated() {
    this.updated = true;
    return this;
  }

  public boolean isUpdated() {
    return updated;
  }

  public KvDocument build() {
    KvDocument updatedDocument = buildRoot();
    clear();
    return updatedDocument;
  }
}
