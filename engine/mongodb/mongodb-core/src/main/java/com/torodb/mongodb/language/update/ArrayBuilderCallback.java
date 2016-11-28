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
class ArrayBuilderCallback implements BuilderCallback<Integer> {

  private final UpdatedToroDocumentArrayBuilder builder;

  public ArrayBuilderCallback(UpdatedToroDocumentArrayBuilder builder) {
    this.builder = builder;
  }

  @Override
  public Class<Integer> getKeyClass() {
    return Integer.class;
  }

  @Override
  public boolean contains(Integer key) {
    return builder.contains(key);
  }

  @Override
  public boolean isValue(Integer key) {
    return builder.isValue(key);
  }

  @Override
  public KvValue<?> getValue(Integer key) {
    return builder.getValue(key);
  }

  @Override
  public boolean isArrayBuilder(Integer key) {
    return builder.isArrayBuilder(key);
  }

  @Override
  public UpdatedToroDocumentArrayBuilder getArrayBuilder(Integer key) {
    return builder.getArrayBuilder(key);
  }

  @Override
  public boolean isObjectBuilder(Integer key) {
    return builder.isObjectBuilder(key);
  }

  @Override
  public UpdatedToroDocumentBuilder getObjectBuilder(Integer key) {
    return builder.getObjectBuilder(key);
  }

  @Override
  public UpdatedToroDocumentArrayBuilder newArray(Integer key) {
    return builder.newArray(key);
  }

  @Override
  public UpdatedToroDocumentBuilder newObject(Integer key) {
    return builder.newObject(key);
  }

  @Override
  public void setValue(Integer key, KvValue<?> value) {
    builder.setValue(key, value);
  }

  @Override
  public boolean unset(Integer key) {
    return builder.unset(key);
  }

}
