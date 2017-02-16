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

package com.torodb.mongodb.repl.oplogreplier.batch;

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.torodb.mongodb.filters.FilterResult;
import com.torodb.mongodb.filters.OplogOperationFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;
import java.util.function.Predicate;

import javax.inject.Inject;

/**
 * A class that, given a {@link OplogOperationFilter}, removes from {@link OplogBatch oplog batches}
 * all elements that do not fulfill the filter.
 */
@SuppressWarnings("serial")
public class OplogBatchFilter implements akka.japi.function.Function<OplogBatch, OplogBatch> {
  private static final Logger LOGGER = LogManager.getLogger(OplogBatchFilter.class);
  private final Predicate<OplogOperation> opPredicate;
  private final Function<OplogOperation, String> unknownReasonFun;

  @Inject
  public OplogBatchFilter(OplogOperationFilter opFilter) {
    this.opPredicate = op -> filterAndLog(opFilter, op);
    this.unknownReasonFun = (op) -> "unknown";
  }

  @Override
  public OplogBatch apply(OplogBatch oplogBatch) {
    return oplogBatch.filter(opPredicate);
  }

  private boolean filterAndLog(OplogOperationFilter opFilter, OplogOperation op) {
    FilterResult<OplogOperation> filterResult = opFilter.apply(op);
    if (filterResult.isSuccessful()) {
      return true;
    }

    LOGGER.debug("Filtered operation {}. Reason: {}", op, filterResult
        .getReason()
        .orElse(unknownReasonFun)
    );
    return false;
  }

}
