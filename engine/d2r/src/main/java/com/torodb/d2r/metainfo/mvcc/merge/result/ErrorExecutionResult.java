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

/**
 * A {@link ExecutionResult} that doesn't success.
 */
class ErrorExecutionResult<P, C> extends ExecutionResult<P> {
  /**
   * Returns the error message on a format that follow {@link MessageFormat} syntax, where
   * the first argument is a text that identifies the parent and the second a text that identifies
   * the changed element.
   */
  private final PrettyErrorMessageFactory<P> messageFactory;

  public ErrorExecutionResult(String ruleId, InnerErrorMessageFactory<P> messageFactory) {
    this.messageFactory = new PrettyErrorMessageFactory<>(ruleId, messageFactory);
  }

  @Override
  public boolean isSuccess() {
    return false;
  }

  @Override
  public Optional<PrettyErrorMessageFactory<P>> getErrorMessageFactory() {
    return Optional.of(messageFactory);
  }
}
