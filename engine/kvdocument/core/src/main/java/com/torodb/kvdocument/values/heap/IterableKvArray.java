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
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.ObjectStreamException;

/**
 *
 */
@SuppressFBWarnings(value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
    justification = "writeReplace is used")
public class IterableKvArray extends KvArray {

  private static final long serialVersionUID = -1955250327304119290L;

  private final Iterable<KvValue<?>> iterable;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
      justification = "We know this can be dangerous, but it improves the efficiency and, by"
      + "contract, the iterable shall be immutable")
  public IterableKvArray(@NotMutable Iterable<KvValue<?>> iterable) {
    this.iterable = iterable;
  }

  @Override
  public UnmodifiableIterator<KvValue<?>> iterator() {
    return Iterators.unmodifiableIterator(iterable.iterator());
  }

  private Object writeReplace() throws ObjectStreamException {
    return new ListKvArray(Lists.newArrayList(this));
  }
}
