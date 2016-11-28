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

import com.torodb.core.exceptions.user.UpdateException;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.values.KvValue;

import java.util.Collection;

/**
 *
 */
public class SetFieldUpdateAction extends SingleFieldUpdateAction implements
    ResolvedCallback<Boolean> {

  private final KvValue<?> newValue;

  public SetFieldUpdateAction(Collection<AttributeReference> modifiedField, KvValue<?> newValue) {
    super(modifiedField);
    this.newValue = newValue;
  }

  public KvValue<?> getNewValue() {
    return newValue;
  }

  @Override
  public void apply(UpdatedToroDocumentBuilder builder) throws UpdateException {
    for (AttributeReference key : getModifiedField()) {
      if (set(new ObjectBuilderCallback(builder), key, newValue)) {
        builder.setUpdated();
        return;
      }
    }
  }

  <K> boolean set(
      BuilderCallback<K> builder,
      Collection<AttributeReference> keys,
      KvValue<?> newValue
  ) throws UpdateException {
    for (AttributeReference key : keys) {
      if (set(builder, key, newValue)) {
        return true;
      }
    }
    return false;
  }

  <K> boolean set(
      BuilderCallback<K> builder,
      AttributeReference key,
      KvValue<?> newValue
  ) throws UpdateException {
    try {
      Boolean result = AttributeReferenceToBuilderCallback.resolve(builder,
          key.getKeys(),
          true,
          this);
      if (result == null) {
        return false;
      }
      return result;
    } catch (UpdateException ex) {
      return false;
    }
  }

  @Override
  public <K> Boolean objectReferenced(
      BuilderCallback<K> parentBuilder,
      K key,
      UpdatedToroDocumentBuilder child
  ) {
    parentBuilder.setValue(key, newValue);
    return true;
  }

  @Override
  public <K> Boolean arrayReferenced(
      BuilderCallback<K> parentBuilder,
      K key,
      UpdatedToroDocumentArrayBuilder child
  ) {
    parentBuilder.setValue(key, newValue);
    return true;
  }

  @Override
  public <K> Boolean valueReferenced(
      BuilderCallback<K> parentBuilder,
      K key,
      KvValue<?> child
  ) {
    parentBuilder.setValue(key, newValue);
    return true;
  }

  @Override
  public <K> Boolean newElementReferenced(
      BuilderCallback<K> parentBuilder,
      K key
  ) {
    parentBuilder.setValue(key, newValue);
    return true;
  }

  @Override
  public <R, A> R accept(UpdateActionVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }
}
