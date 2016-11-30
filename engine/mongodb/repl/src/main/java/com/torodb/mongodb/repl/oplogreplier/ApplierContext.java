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

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;

import java.util.Optional;

/**
 *
 */
public final class ApplierContext {

  private final boolean updatesAsUpserts;
  private final Optional<Boolean> reapplying;

  public ApplierContext(boolean updatesAsUpserts, Optional<Boolean> reapplying) {
    this.updatesAsUpserts = updatesAsUpserts;
    this.reapplying = reapplying;
  }

  /**
   * Returns true if this updates must be executed as upserts.
   *
   * When this is true, {@link UpdateOplogOperation update oplog operations} should never fail.
   *
   * @return
   */
  public boolean treatUpdateAsUpsert() {
    return updatesAsUpserts;
  }

  /**
   * Return an optional whose value indicates if the oplog is being reapplyed.
   *
   * If the retuned optional is not {@link Optional#isPresent()}, then it is unknown if the oplog
   * contains operations that have been already executed.
   *
   * @return
   */
  public Optional<Boolean> isReapplying() {
    return reapplying;
  }

  public static class Builder {

    private boolean updatesAsUpserts = true;
    private Optional<Boolean> reapplying = Optional.empty();

    public Builder() {
    }

    public Builder setUpdatesAsUpserts(boolean updatesAsUpserts) {
      this.updatesAsUpserts = updatesAsUpserts;
      return this;
    }

    public Builder setReapplying(boolean reapplying) {
      this.reapplying = Optional.of(reapplying);
      return this;
    }

    public Builder setReapplying(Optional<Boolean> reaplying) {
      this.reapplying = reaplying;
      return this;
    }

    public ApplierContext build() {
      return new ApplierContext(updatesAsUpserts, reapplying);
    }
  }
}
