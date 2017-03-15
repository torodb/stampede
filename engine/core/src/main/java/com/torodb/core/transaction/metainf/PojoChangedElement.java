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

package com.torodb.core.transaction.metainf;

import org.jooq.lambda.tuple.Tuple2;

/**
 *
 */
public class PojoChangedElement<E> implements ChangedElement<E> {

  private final E element;
  private final MetaElementState change;

  public PojoChangedElement(E element, MetaElementState change) {
    this.element = element;
    this.change = change;
  }

  public PojoChangedElement(Tuple2<E, MetaElementState> tuple) {
    this.element = tuple.v1();
    this.change = tuple.v2();
  }

  @Override
  public E getElement() {
    return element;
  }

  @Override
  public MetaElementState getChange() {
    return change;
  }

}
