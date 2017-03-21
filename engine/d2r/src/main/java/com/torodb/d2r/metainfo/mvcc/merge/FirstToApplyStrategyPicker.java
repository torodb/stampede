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

package com.torodb.d2r.metainfo.mvcc.merge;

import com.torodb.core.d2r.D2RLoggerFactory;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Optional;

/**
 * A {@link MergeStrategyPicker} that contains a list of partial strategies and picks the first
 * strategy that 
 * {@link PartialMergeStrategy#appliesTo(com.torodb.metainfo.cache.mvcc.merge.MergeContext) applies}
 * on the given context.
 * @param <P> the type of the commited parent
 * @param <C> the type of the changed element
 * @param <PBT> the builder class to create new parents
 * @param <CtxT> the context type
 */
public class FirstToApplyStrategyPicker<P, C, PBT, CtxT extends MergeContext<P, C>>
    implements MergeStrategyPicker<P, C, PBT, CtxT> {

  private static final Logger LOGGER = D2RLoggerFactory.get(FirstToApplyStrategyPicker.class);
  private final Collection<PartialMergeStrategy<P, C, PBT, CtxT>> strategies;
  private final MergeStrategy<P, C, PBT, CtxT> onOtherCase;

  public FirstToApplyStrategyPicker(Collection<PartialMergeStrategy<P, C, PBT, CtxT>> strategies) {
    this(strategies, new DoNothingMergeStrategy<>());
  }

  public FirstToApplyStrategyPicker(Collection<PartialMergeStrategy<P, C, PBT, CtxT>> strategies,
      MergeStrategy<P, C, PBT, CtxT> onOtherCase) {
    this.strategies = strategies;
    this.onOtherCase = onOtherCase;
  }

  @Override
  public MergeStrategy<P, C, PBT, CtxT> pick(CtxT context) {
    Optional<PartialMergeStrategy<P, C, PBT, CtxT>> subStrategy = strategies.stream()
        .filter(s -> filter(s, context))
        .findFirst();
    if (subStrategy.isPresent()) {
      return subStrategy.get();
    } else {
      LOGGER.debug("As no other strategy applies, the default strategy {} is used to execute {}",
          onOtherCase, context);
      return onOtherCase;
    }
  }

  private boolean filter(PartialMergeStrategy<P, C, PBT, CtxT> strategy, CtxT context) {
    boolean applies = strategy.appliesTo(context);
    if (applies) {
      LOGGER.debug("Using {} to execute {}", strategy, context);
    } else {
      LOGGER.trace("Strategy {} does not apply to {}", strategy, context);
    }
    return applies;
  }
}
