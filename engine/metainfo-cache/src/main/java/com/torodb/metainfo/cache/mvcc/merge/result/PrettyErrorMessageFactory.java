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

package com.torodb.metainfo.cache.mvcc.merge.result;

import java.util.function.Function;

/**
 * The function returned by {@link ExecutionResult#getErrorMessageFactory() } so the error text
 * can be generated.
 */
public class PrettyErrorMessageFactory<P> implements Function<ParentDescriptionFun<P>, String> {

  private final String ruleId;
  private final InnerErrorMessageFactory<P> delegate;

  public PrettyErrorMessageFactory(String ruleId, InnerErrorMessageFactory<P> delegate) {
    this.ruleId = ruleId;
    this.delegate = delegate;
  }

  @Override
  public String apply(ParentDescriptionFun<P> parentDescriptionFun) {
    return ruleId + ": " + delegate.apply(parentDescriptionFun);
  }

  public <P2> PrettyErrorMessageFactory<P2> transform(
      Function<InnerErrorMessageFactory<P>, InnerErrorMessageFactory<P2>> transformation) {
    return new PrettyErrorMessageFactory<>(ruleId, transformation.apply(delegate));
  }

}
