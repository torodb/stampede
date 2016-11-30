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

package com.torodb.core.util;

import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.ArrayKey;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;

import java.util.List;
import java.util.Optional;

/**
 *
 */
public class AttributeRefKvDocResolver {

  private AttributeRefKvDocResolver() {
  }

  public static Optional<KvValue<?>> resolve(AttributeReference attRef, KvDocument in) {
    List<Key<?>> keys = attRef.getKeys();
    return resolve(keys, in, 0);
  }

  private static Optional<KvValue<?>> resolve(List<Key<?>> keyList, KvDocument in, int pos) {
    Key<?> key = keyList.get(pos);
    if (!(key instanceof ObjectKey)) {
      return Optional.empty();
    } else {
      KvValue<?> keyValue = in.get(((ObjectKey) key).getKey());

      if (keyValue == null) {
        return Optional.empty();
      }
      if (pos == keyList.size() - 1) {
        return Optional.of(keyValue);
      }
      if (keyValue instanceof KvDocument) {
        return resolve(keyList, (KvDocument) keyValue, pos + 1);
      }
      if (keyValue instanceof KvArray) {
        return resolve(keyList, (KvArray) keyValue, pos + 1);
      }
      return Optional.empty();
    }
  }

  private static Optional<KvValue<?>> resolve(List<Key<?>> keyList, KvArray in, int pos) {
    Key<?> key = keyList.get(pos);
    if (!(key instanceof ArrayKey)) {
      return Optional.empty();
    } else {
      int index = ((ArrayKey) key).getIndex();
      if (index < 0 || index >= in.size()) {
        return Optional.empty();
      }
      KvValue<?> keyValue = in.get(index);
      if (pos == keyList.size()) {
        return Optional.of(keyValue);
      }
      if (keyValue instanceof KvDocument) {
        return resolve(keyList, (KvDocument) keyValue, pos + 1);
      }
      if (keyValue instanceof KvArray) {
        return resolve(keyList, (KvArray) keyValue, pos + 1);
      }
      return Optional.empty();
    }
  }

}
