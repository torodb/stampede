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

package com.torodb.d2r.metainfo.mvcc.merge.result;

import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link ExecutionResult} that maps the error message to a delegate.
 */
class MapErrorExecutionResult<P1, P2> extends ExecutionResult<P2> {

  private final ExecutionResult<P1> delegate;
  private final Function<InnerErrorMessageFactory<P1>, InnerErrorMessageFactory<P2>> transformation;

  MapErrorExecutionResult(ExecutionResult<P1> delegate,
      Function<InnerErrorMessageFactory<P1>, InnerErrorMessageFactory<P2>> transformation) {
    this.delegate = delegate;
    this.transformation = transformation;
  }

  @Override
  public boolean isSuccess() {
    return delegate.isSuccess();
  }

  @Override
  public Optional<PrettyErrorMessageFactory<P2>> getErrorMessageFactory() {
    return delegate.getErrorMessageFactory()
        .map(factory -> factory.transform(transformation));
  }

}
