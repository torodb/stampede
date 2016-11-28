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
public class UnsetFieldUpdateAction extends SingleFieldUpdateAction implements
    ResolvedCallback<Boolean> {

  public UnsetFieldUpdateAction(Collection<AttributeReference> modifiedField) {
    super(modifiedField);
  }

  @Override
  public void apply(UpdatedToroDocumentBuilder builder) throws UpdateException {
    for (AttributeReference key : getModifiedField()) {
      if (unset(new ObjectBuilderCallback(builder), key)) {
        builder.setUpdated();
        return;
      }
    }
  }

  <K> boolean unset(
      BuilderCallback<K> builder,
      Collection<AttributeReference> keys
  ) throws UpdateException {
    for (AttributeReference key : keys) {
      if (unset(builder, key)) {
        return true;
      }
    }
    return false;
  }

  <K> boolean unset(
      BuilderCallback<K> builder,
      AttributeReference key
  ) throws UpdateException {
    Boolean result = AttributeReferenceToBuilderCallback.resolve(
        builder,
        key.getKeys(),
        false,
        this
    );
    if (result == null) {
      return false;
    }
    return result;
  }

  @Override
  public <K> Boolean objectReferenced(
      BuilderCallback<K> parentBuilder,
      K key,
      UpdatedToroDocumentBuilder child
  ) {
    parentBuilder.unset(key);
    return true;
  }

  @Override
  public <K> Boolean arrayReferenced(
      BuilderCallback<K> parentBuilder,
      K key,
      UpdatedToroDocumentArrayBuilder child
  ) {
    parentBuilder.unset(key);
    return true;
  }

  @Override
  public <K> Boolean valueReferenced(
      BuilderCallback<K> parentBuilder,
      K key,
      KvValue<?> child
  ) {
    parentBuilder.unset(key);
    return true;
  }

  @Override
  public <K> Boolean newElementReferenced(
      BuilderCallback<K> parentBuilder,
      K key
  ) {
    return false;
  }

  @Override
  public <R, A> R accept(UpdateActionVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }
}
