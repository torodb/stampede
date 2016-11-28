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

import com.torodb.core.annotations.DoNotChange;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public interface MetainfoRepository {

  /**
   * Starts a snapshot stage that will be active meanwhile the stage is not
   * {@link SnapshotStage#close() closed}.
   *
   * @return
   */
  public SnapshotStage startSnapshotStage();

  /**
   * Starts a merging stage that will be active meanwhile the stage is not
   * {@link MergerStage#close() closed}.
   *
   * @param snapshot a mutable snapshot whose changes will be added to metainfo managed by this
   *                 object.
   * @return
   * @throws IllegalArgumentException if the given snapshot is not related with this object.
   * @throws UnmergeableException     if the given snapshot is incompatible with the current
   *                                  snapshot
   */
  public MergerStage startMerge(MutableMetaSnapshot snapshot) throws IllegalArgumentException,
      UnmergeableException;

  @NotThreadSafe
  public static interface SnapshotStage extends AutoCloseable {

    /**
     * Creates a {@link ImmutableMetaSnapshot} that will remain constant even if other concurrent
     * threads modifies their snapshots and merge their changes.
     *
     * The returned snapshot can be used even after this stage is closed. In fact, it is a good
     * practice to close this stage as soon as no more snapshots are needed.
     *
     * @return
     */
    @DoNotChange
    public ImmutableMetaSnapshot createImmutableSnapshot();

    /**
     * Creates a {@link MutableMetaSnapshot} that will be isolated of other concurrent threads that
     * modifies their snapshots and merge their changes.
     *
     * The returned snapshot must not be used until this stage is closed.
     *
     * @return
     */
    public MutableMetaSnapshot createMutableSnapshot();

    /**
     * Closes the stage.
     *
     * After this method is called, it is illegal to call {@link #createMutableSnapshot() } or
         * {@link #createImmutableSnapshot() }, but previously created snapshots can be still used.
     *
     * This method should never fail due to business conditions (including concurrent threads that
     * access or modify the snapshot by other {@link MergerStage} or {@link SnapshotStage}).
     */
    @Override
    public void close();
  }

  @NotThreadSafe
  public static interface MergerStage extends AutoCloseable {

    /**
     * Commits the merger stage and stores all its changes on the repository.
     *
     * This method should never fail due to business conditions (like incomatible metainfo changes
     * on the same or concurrent threads that uses other {@link MergerStage} or
     * {@link SnapshotStage}).
     */
    public void commit();

    /**
     * Closes the stage.
     *
     * All changes that wont be stored on the repository.
     */
    @Override
    public void close();

  }

}
