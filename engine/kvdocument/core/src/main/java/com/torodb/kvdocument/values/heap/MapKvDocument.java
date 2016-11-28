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

package com.torodb.kvdocument.values.heap;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;

import java.util.LinkedHashMap;

/**
 *
 */
public class MapKvDocument extends KvDocument {

  private static final long serialVersionUID = -5654643148723237245L;

  private final LinkedHashMap<String, KvValue<?>> map;

  public MapKvDocument(@NotMutable LinkedHashMap<String, KvValue<?>> map) {
    this.map = map;
  }

  @Override
  public UnmodifiableIterator<DocEntry<?>> iterator() {
    return Iterators.unmodifiableIterator(
        Iterators.transform(map.entrySet().iterator(), KvDocument.FromEntryMap.INSTANCE)
    );
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(String key) {
    return map.containsKey(key);
  }

  @Override
  public KvValue<?> get(String key) {
    return map.get(key);
  }
}
