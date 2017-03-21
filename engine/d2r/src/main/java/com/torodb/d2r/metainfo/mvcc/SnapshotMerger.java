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

package com.torodb.d2r.metainfo.mvcc;

import com.torodb.core.d2r.D2RLoggerFactory;
import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.d2r.metainfo.mvcc.merge.db.DatabaseMergeStrategy;
import com.torodb.d2r.metainfo.mvcc.merge.db.DbContext;
import com.torodb.d2r.metainfo.mvcc.merge.result.ExecutionResult;
import com.torodb.d2r.metainfo.mvcc.merge.result.ParentDescriptionFun;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * The class used to merge the last commited {@link ImmutableMetaSnapshot} with a uncommited
 * {@link MutableMetaSnapshot}.
 */
public class SnapshotMerger {

  private static final Logger LOGGER = D2RLoggerFactory.get(SnapshotMerger.class);
  private static final DatabaseMergeStrategy DB_MERGE_STRATEGY = new DatabaseMergeStrategy();
  private final ImmutableMetaSnapshot oldSnapshot;
  private final MutableMetaSnapshot newSnapshot;

  public SnapshotMerger(ImmutableMetaSnapshot oldSnapshot, MutableMetaSnapshot newSnapshot) {
    this.oldSnapshot = oldSnapshot;
    this.newSnapshot = newSnapshot;
  }

  /**
   * Merge the uncommited snapshot into the commited one.
   *
   * @return a {@link ImmutableMetaSnapshot.Builder} that can be used to create the merged snapshot.
   * @throws UnmergeableException if there is an incompatibility that makes impossible to merge the
   *                              uncommited into the commited snapshot
   */
  public ImmutableMetaSnapshot.Builder merge() throws UnmergeableException {

    ImmutableMetaSnapshot.Builder builder = new ImmutableMetaSnapshot.Builder(oldSnapshot);

    ExecutionResult<ImmutableMetaSnapshot> result = newSnapshot.streamModifiedDatabases()
        .map(change -> mergeDb(change, builder))
        .filter(dbResult -> !dbResult.isSuccess())
        .findAny()
        .orElse(ExecutionResult.success());

    checkLegacy(result, builder)
        .ifPresent(diff -> LOGGER.warn(diff));

    if (!result.isSuccess()) {
      throw new UnmergeableException(oldSnapshot, newSnapshot, getErrorMessage(result));
    }

    return builder;
  }

  private ExecutionResult<ImmutableMetaSnapshot> mergeDb(
      ChangedElement<MutableMetaDatabase> change, ImmutableMetaSnapshot.Builder parentBuilder) {

    DbContext ctx = new DbContext(oldSnapshot, change);

    return DB_MERGE_STRATEGY.execute(ctx, parentBuilder);
  }

  private String getErrorMessage(ExecutionResult<ImmutableMetaSnapshot> result) {
    ParentDescriptionFun<ImmutableMetaSnapshot> snapshotDescFun = (snapshot) -> "";
    return result.getErrorMessageFactory()
        .map(errFactory -> errFactory.apply(snapshotDescFun))
        .orElse("unknown");
  }

  private Optional<String> checkLegacy(
      ExecutionResult<ImmutableMetaSnapshot> newResult,
      ImmutableMetaSnapshot.Builder builder) {

    UnmergeableException error;
    try {
      legacyMerge();
      error = null;
    } catch (UnmergeableException ex) {
      error = ex;
    }

    if (error != null && newResult.isSuccess()) {
      return Optional.of("Legacy merger found and error not reported by the strategy merger. "
          + "Error found: " + error.getMessage());
    }
    if (error == null && !newResult.isSuccess()) {
      return Optional.of(
          "Strategy merger found and error not reported by the legacy merger. "
          + "Error found: " + getErrorMessage(newResult));
    }
    if (error != null && !newResult.isSuccess()) {
      return Optional.empty();
    }
    //To be totally correct, we should check that generated immutables are the same
    return Optional.empty();
  }

  private ImmutableMetaSnapshot legacyMerge() throws UnmergeableException {
    LegacySnapshotMerger legacyMerger = new LegacySnapshotMerger(oldSnapshot, newSnapshot);

    return legacyMerger.merge().build();
  }
}
