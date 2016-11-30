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

package com.torodb.mongodb.repl.topology;

import java.io.Serializable;
import java.time.Duration;

/**
 *
 */
class HeartbeatResponseAction implements Serializable {

  private static final long serialVersionUID = 434854838132840069L;

  private final Action action;
  private final int primaryIndex;
  private Duration nextHeartbeatDelay;

  private HeartbeatResponseAction(Action action) {
    this(action, -1);
  }

  private HeartbeatResponseAction(Action action, int primaryIndex) {
    this.action = action;
    this.primaryIndex = primaryIndex;
    this.nextHeartbeatDelay = Duration.ZERO;
  }

  public static HeartbeatResponseAction makeNoAction() {
    return new HeartbeatResponseAction(Action.NO_ACTION);
  }

  public static HeartbeatResponseAction makeReconfigAction() {
    return new HeartbeatResponseAction(Action.RECONFIG);
  }

  public static HeartbeatResponseAction makeElectAction() {
    return new HeartbeatResponseAction(Action.START_ELECTION);
  }

  public static HeartbeatResponseAction makeStepDownSelfAction(int primaryIndex) {
    return new HeartbeatResponseAction(Action.STEP_DOWN_SELF, primaryIndex);
  }

  public static HeartbeatResponseAction makeStepDownRemoteAction(int primaryIndex) {
    return new HeartbeatResponseAction(Action.STEP_DOWN_REMOTE_PRIMARY, primaryIndex);
  }

  public Action getAction() {
    return action;
  }

  public int getPrimaryIndex() {
    return primaryIndex;
  }

  public Duration getNextHeartbeatDelay() {
    return nextHeartbeatDelay;
  }

  HeartbeatResponseAction setNextHeartbeatDelay(Duration delay) {
    this.nextHeartbeatDelay = delay;
    return this;
  }

  static enum Action {
    NO_ACTION,
    RECONFIG,
    START_ELECTION,
    STEP_DOWN_SELF,
    STEP_DOWN_REMOTE_PRIMARY
  }

}
