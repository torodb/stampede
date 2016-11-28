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

package com.torodb.core.exceptions.user;

import com.torodb.kvdocument.values.KvValue;

import javax.annotation.Nullable;

/**
 *
 */
public class UniqueIndexViolationException extends IndexException {

  private static final long serialVersionUID = 1;

  @Nullable
  private final KvValue<?> repeatedValue;

  public UniqueIndexViolationException(String index, KvValue<?> repeatedValue) {
    super(null, null, index);
    this.repeatedValue = repeatedValue;
  }

  public UniqueIndexViolationException(String index, KvValue<?> repeatedValue, String message) {
    super(message, null, null, index);
    this.repeatedValue = repeatedValue;
  }

  public UniqueIndexViolationException(String index, KvValue<?> repeatedValue, String message,
      Throwable cause) {
    super(message, cause, null, null, index);
    this.repeatedValue = repeatedValue;
  }

  public UniqueIndexViolationException(String message) {
    super(message, null, null, null);
    this.repeatedValue = null;
  }

  public UniqueIndexViolationException(String message, Throwable cause) {
    super(message, cause, null, null, null);
    this.repeatedValue = null;
  }

  @Nullable
  public KvValue<?> getRepeatedValue() {
    return repeatedValue;
  }

  @Override
  public <R, A> R accept(UserExceptionVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

}
