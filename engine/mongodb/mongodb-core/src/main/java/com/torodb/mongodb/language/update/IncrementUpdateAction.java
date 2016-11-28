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
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvNumeric;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.KvValueAdaptor;

import java.util.Collection;

/**
 *
 */
public class IncrementUpdateAction extends SingleFieldUpdateAction implements
    ResolvedCallback<Boolean> {

  private static final Incrementer INCREMENTER = new Incrementer();

  private final KvNumeric<?> delta;

  public IncrementUpdateAction(Collection<AttributeReference> modifiedField, KvNumeric<?> delta) {
    super(modifiedField);
    this.delta = delta;
  }

  public KvNumeric<?> getDelta() {
    return delta;
  }

  @Override
  public void apply(UpdatedToroDocumentBuilder builder) throws UpdateException {
    for (AttributeReference key : getModifiedField()) {
      if (increment(new ObjectBuilderCallback(builder), key, delta)) {
        builder.setUpdated();
        return;
      }
    }
  }

  <K> boolean increment(
      BuilderCallback<K> builder,
      AttributeReference key,
      KvNumeric<?> delta
  ) throws UpdateException {
    Boolean result = AttributeReferenceToBuilderCallback.resolve(builder,
        key.getKeys(),
        true,
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
  ) throws UpdateException {
    throw new UpdateException(
        "Cannot increment a value of a non-numeric type. "
        + parentBuilder + " has the field '" + key + "' of "
        + "non-numeric type object");
  }

  @Override
  public <K> Boolean arrayReferenced(
      BuilderCallback<K> parentBuilder,
      K key,
      UpdatedToroDocumentArrayBuilder child
  ) throws UpdateException {
    throw new UpdateException(
        "Cannot increment a value of a non-numeric type. "
        + parentBuilder + " has the field '" + key + "' of "
        + "non-numeric type array");
  }

  @Override
  public <K> Boolean valueReferenced(
      BuilderCallback<K> parentBuilder,
      K key,
      KvValue<?> child
  ) throws UpdateException {
    if (!(child instanceof KvNumeric)) {
      throw new UpdateException(
          "Cannot increment a value of a non-numeric type. "
          + parentBuilder + " has the field '" + key + "' of "
          + "non-numeric type " + child.getType());
    }
    KvNumeric<?> numericChild = (KvNumeric<?>) child;
    parentBuilder.setValue(key, numericChild.accept(INCREMENTER, delta));
    return true;
  }

  @Override
  public <K> Boolean newElementReferenced(
      BuilderCallback<K> parentBuilder,
      K key
  ) {
    parentBuilder.setValue(key, delta);
    return true;
  }

  @Override
  public <R, A> R accept(UpdateActionVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  private static class Incrementer extends KvValueAdaptor<KvNumeric<?>, KvNumeric<?>> {

    @Override
    public KvNumeric<?> visit(KvDouble value, KvNumeric<?> arg) {
      return KvDouble.of(value.doubleValue() + arg.doubleValue());
    }

    @Override
    public KvNumeric<?> visit(KvLong value, KvNumeric<?> arg) {
      return KvDouble.of(value.doubleValue() + arg.doubleValue());
    }

    @Override
    public KvNumeric<?> visit(KvInteger value, KvNumeric<?> arg) {
      return KvDouble.of(value.doubleValue() + arg.doubleValue());
    }

    @Override
    public KvNumeric<?> defaultCase(KvValue<?> value, KvNumeric<?> arg) {
      throw new AssertionError("Trying to increment a value of type " + value.getType()
          + " which is not a number");
    }

  }

}
