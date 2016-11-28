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

import javax.annotation.Nonnull;

/**
 *
 */
public enum MetaElementState {
  NOT_EXISTENT,
  NOT_CHANGED,
  ADDED,
  MODIFIED,
  REMOVED;

  boolean hasChanged() {
    switch (this) {
      case REMOVED:
      case ADDED:
      case MODIFIED:
        return true;
      case NOT_EXISTENT:
      case NOT_CHANGED:
        return false;
      default:
        throw new AssertionError("Illegal state " + this);
    }
  }

  boolean isAlive() {
    switch (this) {
      case NOT_CHANGED:
      case ADDED:
      case MODIFIED:
        return true;
      case NOT_EXISTENT:
      case REMOVED:
        return false;
      default:
        throw new AssertionError("Illegal state " + this);
    }
  }

  public void assertLegalTransition(@Nonnull MetaElementState newState) {
    assert this.isLegalTransition(newState) :
        "It is not legal to transist from " + this + " to " + newState;
  }

  public boolean isLegalTransition(@Nonnull MetaElementState newState) {
    if (!newState.hasChanged()) {
      return false;
    }
    switch (this) {
      case NOT_EXISTENT:
      case REMOVED:
        return newState == ADDED;
      case NOT_CHANGED:
      case ADDED:
        return newState == MODIFIED || newState == REMOVED;
      case MODIFIED:
        return newState == REMOVED || newState == MODIFIED;
      default:
        throw new AssertionError("Unexpected change " + newState);
    }
  }

}
