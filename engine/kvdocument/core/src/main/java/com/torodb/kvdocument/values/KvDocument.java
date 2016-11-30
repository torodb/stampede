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

package com.torodb.kvdocument.values;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.hash.Hashing;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.values.KvDocument.DocEntry;
import com.torodb.kvdocument.values.heap.InstantKvInstant;
import com.torodb.kvdocument.values.heap.LocalDateKvDate;
import com.torodb.kvdocument.values.heap.LocalTimeKvTime;
import com.torodb.kvdocument.values.heap.MapKvDocument;
import com.torodb.kvdocument.values.heap.StringKvString;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class KvDocument extends KvValue<KvDocument> implements Iterable<DocEntry<?>> {

  private static final long serialVersionUID = 3113643174084420474L;

  @Override
  public abstract UnmodifiableIterator<DocEntry<?>> iterator();

  @Override
  public KvDocument getValue() {
    return this;
  }

  @Override
  public Class<? extends KvDocument> getValueClass() {
    return getClass();
  }

  @Override
  public DocumentType getType() {
    return DocumentType.INSTANCE;
  }

  @Override
  public String toString() {
    StringBuilder toStringBuilder = new StringBuilder(
        Iterables.toString(this));

    toStringBuilder.setCharAt(0, '{');
    toStringBuilder.setCharAt(toStringBuilder.length() - 1, '}');

    return toStringBuilder.toString();
  }

  /**
   *
   * @param key
   * @return the value associated with that key or null if there is no entry with that key
   */
  @Nullable
  public KvValue<?> get(String key) {
    for (DocEntry<?> entry : this) {
      if (entry.getKey().equals(key)) {
        return entry.getValue();
      }
    }
    return null;
  }

  public Iterable<String> getKeys() {
    return Iterables.transform(this, new ExtractKeyFunction());
  }

  public boolean containsKey(String key) {
    return get(key) != null;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int size() {
    return Iterables.size(this);
  }

  /**
   * @return the first entry of this document
   * @throws NoSuchElementException if it is {@linkplain #isEmpty() empty}
   */
  public DocEntry<?> getFirstEntry() throws NoSuchElementException {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return iterator().next();
  }

  /**
   * Two documents are equal if they contain the same entries in the same order.
   *
   * @param obj
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvDocument)) {
      return false;
    }
    KvDocument other = (KvDocument) obj;

    return Iterables.elementsEqual(this, other);
  }

  @Override
  public int hashCode() {
    return Hashing.goodFastHash(32).hashInt(size()).asInt();
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  public abstract static class DocEntry<V> {

    public abstract String getKey();

    public abstract KvValue<V> getValue();

    /**
     * Two entries are equals if their keys and values are equal.
     *
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof DocEntry)) {
        return false;
      }
      DocEntry<?> other = (DocEntry<?>) obj;
      return this.getKey().equals(other.getKey()) && this.getValue().equals(other.getValue());
    }

    /**
     * The hashCode of a entry is the hash of its key.
     *
     * @return
     */
    @Override
    public int hashCode() {
      return getKey().hashCode();
    }

    @Override
    public String toString() {
      return getKey() + " : " + getValue();
    }
  }

  public static class SimpleDocEntry<V> extends DocEntry<V> {

    private final String key;
    private final KvValue<V> value;

    public SimpleDocEntry(String key, KvValue<V> value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public KvValue<V> getValue() {
      return value;
    }
  }

  public static class Builder {

    private final LinkedHashMap<String, KvValue<?>> values = Maps.newLinkedHashMap();
    private boolean built = false;

    public Builder putValue(String key, KvValue<?> value) {
      checkBuilt();
      values.put(key, value);
      return this;
    }

    public Builder putValue(String key, boolean value) {
      return putValue(key, KvBoolean.from(value));
    }

    public Builder putValue(String key, Instant value) {
      return putValue(key, new InstantKvInstant(value));
    }

    public Builder putValue(String key, LocalDate value) {
      return putValue(key, new LocalDateKvDate(value));
    }

    public Builder putValue(String key, double value) {
      return putValue(key, KvDouble.of(value));
    }

    public Builder putValue(String key, int value) {
      return putValue(key, KvInteger.of(value));
    }

    public Builder putValue(String key, long value) {
      return putValue(key, KvLong.of(value));
    }

    public Builder putValue(String key, String value) {
      return putValue(key, new StringKvString(value));
    }

    public Builder putValue(String key, LocalTime value) {
      return putValue(key, new LocalTimeKvTime(value));
    }

    public KvDocument build() {
      checkBuilt();
      built = true;
      return new MapKvDocument(values);
    }

    private void checkBuilt() {
      if (built) {
        throw new IllegalStateException("This builder has been already used");
      }
    }
    
    public Builder putNullValue(String key) {
      return putValue(key, KvNull.getInstance());
    }

  }

  protected static class FromEntryMap implements
      Function<Map.Entry<String, KvValue<?>>, DocEntry<?>> {

    public static final FromEntryMap INSTANCE = new FromEntryMap();

    private FromEntryMap() {
    }

    @Override
    public DocEntry<?> apply(@Nonnull Map.Entry<String, KvValue<?>> input) {
      return new SimpleDocEntry<>(input.getKey(), input.getValue());
    }

  }

  private static class ExtractKeyFunction implements Function<DocEntry<?>, String> {

    public ExtractKeyFunction() {
    }

    @Override
    public String apply(@Nonnull DocEntry<?> input) {
      return input.getKey();
    }
  }
}
