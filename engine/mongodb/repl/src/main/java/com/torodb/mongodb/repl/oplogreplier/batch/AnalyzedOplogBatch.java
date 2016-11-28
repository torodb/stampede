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

import java.util.Objects;

public abstract class AnalyzedOplogBatch {

  private final Object batchId = new Object();

  public Object getBatchId() {
    return batchId;
  }

  public abstract <R, A, T extends Throwable> R accept(
      AnalyzedOplogBatchVisitor<R, A, T> visitor, A arg) throws T;

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 79 * hash + Objects.hashCode(this.batchId);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final AnalyzedOplogBatch other = (AnalyzedOplogBatch) obj;
    if (!Objects.equals(this.batchId, other.batchId)) {
      return false;
    }
    return true;
  }
}
