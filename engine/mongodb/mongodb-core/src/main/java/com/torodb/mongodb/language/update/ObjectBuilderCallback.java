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

import com.torodb.kvdocument.values.KvValue;

/**
 *
 */
class ObjectBuilderCallback implements BuilderCallback<String> {

  private final UpdatedToroDocumentBuilder builder;

  public ObjectBuilderCallback(UpdatedToroDocumentBuilder builder) {
    this.builder = builder;
  }

  @Override
  public Class<String> getKeyClass() {
    return String.class;
  }

  @Override
  public boolean contains(String key) {
    return builder.contains(key);
  }

  @Override
  public boolean isValue(String key) {
    return builder.isValue(key);
  }

  @Override
  public KvValue<?> getValue(String key) {
    return builder.getValue(key);
  }

  @Override
  public boolean isArrayBuilder(String key) {
    return builder.isArrayBuilder(key);
  }

  @Override
  public UpdatedToroDocumentArrayBuilder getArrayBuilder(String key) {
    return builder.getArrayBuilder(key);
  }

  @Override
  public boolean isObjectBuilder(String key) {
    return builder.isObjectBuilder(key);
  }

  @Override
  public UpdatedToroDocumentBuilder getObjectBuilder(String key) {
    return builder.getObjectBuilder(key);
  }

  @Override
  public UpdatedToroDocumentArrayBuilder newArray(String key) {
    return builder.newArray(key);
  }

  @Override
  public UpdatedToroDocumentBuilder newObject(String key) {
    return builder.newObject(key);
  }

  @Override
  public void setValue(String key, KvValue<?> value) {
    builder.putValue(key, value);
  }

  @Override
  public boolean unset(String key) {
    return builder.unset(key);
  }

}
