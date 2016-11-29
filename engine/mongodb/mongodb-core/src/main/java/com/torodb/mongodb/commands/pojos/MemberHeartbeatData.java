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

package com.torodb.mongodb.commands.pojos;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatReply;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatReplyBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class contains the data returned from a heartbeat command for one member of a replica set.
 */
//TODO(gortiz): this class generates some extra optionals that can be optimized
public class MemberHeartbeatData {

  private static final Logger LOGGER = LogManager.getLogger(MemberHeartbeatData.class);
  private Health health;
  @Nullable
  private Instant upSince;
  private Instant lastHeartbeat;
  @Nullable
  private Instant lastHeartbeatRecv;
  private boolean authIssue;
  @Nonnull
  private ReplSetHeartbeatReply lastResponse;

  public MemberHeartbeatData() {
    this.health = Health.NOT_CHECKED;
    this.upSince = Instant.EPOCH;
    this.lastHeartbeat = Instant.EPOCH;
    this.lastHeartbeatRecv = Instant.EPOCH;
    this.authIssue = false;

    lastResponse = new ReplSetHeartbeatReplyBuilder()
        .setSetName("_unnamed")
        .setElectionTime(DefaultBsonValues.newTimestamp(0, 0))
        .setState(MemberState.RS_UNKNOWN)
        .setAppliedOpTime(OpTime.EPOCH)
        .build();
  }

  public MemberHeartbeatData(
      Health health,
      @Nullable Instant upSince,
      Instant lastHeartbeat,
      @Nullable Instant lastHeartbeatRecv,
      boolean authIssue,
      ReplSetHeartbeatReply lastResponse) {
    this.health = health;
    this.upSince = upSince;
    this.lastHeartbeat = lastHeartbeat;
    this.lastHeartbeatRecv = lastHeartbeatRecv;
    this.authIssue = authIssue;
    this.lastResponse = lastResponse;
  }

  public Health getHealth() {
    return health;
  }

  @Nullable
  public Instant getUpSince() {
    return upSince;
  }

  public Instant getLastHeartbeat() {
    return lastHeartbeat;
  }

  /**
   * Returns the instant when we recived the last heartbeat from this node.
   *
   * @return the instant when we recived the last heartbeat from this node.
   */
  @Nullable
  public Instant getLastHeartbeatRecv() {
    return lastHeartbeatRecv;
  }

  public boolean isAuthIssue() {
    return authIssue;
  }

  public ReplSetHeartbeatReply getLastResponse() {
    return lastResponse;
  }

  @Nullable
  public MemberState getState() {
    return lastResponse.getState().orElse(null);
  }

  @Nullable
  public OpTime getOpTime() {
    return lastResponse.getAppliedOpTime().orElse(null);
  }

  @Nonnull
  public String getLastHeartbeatMessage() {
    return lastResponse.getHbmsg();
  }

  @Nullable
  public HostAndPort getSyncSource() {
    return lastResponse.getSyncingTo().orElse(null);
  }

  @Nullable
  public BsonTimestamp getElectionTime() {
    return lastResponse.getElectionTime().orElse(null);
  }

  public long getConfigVersion() {
    return lastResponse.getConfigVersion();
  }

  /**
   * @return true iff the member is up or if no heartbeat has been received from him yet.
   */
  public boolean maybeUp() {
    return health != Health.NOT_CHECKED;
  }

  public boolean isUp() {
    return health == Health.UP;
  }

  public boolean isUnelectable() {
    return lastResponse.getElectable().get();
  }

  public void setUpValues(@Nonnull Instant now, @Nonnull HostAndPort host,
      @Nonnull ReplSetHeartbeatReply hbResponse) {
    health = Health.UP;
    if (upSince.equals(Instant.EPOCH)) {
      upSince = now;
    }
    authIssue = false;
    lastHeartbeat = now;

    ReplSetHeartbeatReplyBuilder lastResponseBuilder = new ReplSetHeartbeatReplyBuilder(
        hbResponse, lastResponse);

    // Log if the state changes
    if (!lastResponse.getState().get().equals(hbResponse.getState().get())) {
      LOGGER.info("Member {} is now in state {}", host, hbResponse.getState().get());
    }

    lastResponse = lastResponseBuilder.build();
  }

  public void setAuthIssue(Instant now) {
    health = Health.UNREACHABLE;  // set health to 0 so that this doesn't count towards majority.
    upSince = Instant.EPOCH;
    lastHeartbeat = now;
    authIssue = true;

    lastResponse = new ReplSetHeartbeatReplyBuilder()
        .setSetName(lastResponse.getSetName())
        .setElectionTime(DefaultBsonValues.newTimestamp(0, 0))
        .setState(MemberState.RS_UNKNOWN)
        .setAppliedOpTime(OpTime.EPOCH)
        .setSyncingTo(null)
        .build();
  }

  public void setDownValues(Instant now, @Nonnull String errorDesc) {
    health = Health.UNREACHABLE;  // set health to 0 so that this doesn't count towards majority.
    upSince = Instant.EPOCH;
    lastHeartbeat = now;
    authIssue = false;

    lastResponse = new ReplSetHeartbeatReplyBuilder()
        .setSetName(lastResponse.getSetName())
        .setElectionTime(DefaultBsonValues.newTimestamp(0, 0))
        .setHbmsg(errorDesc)
        .setState(MemberState.RS_DOWN)
        .setAppliedOpTime(OpTime.EPOCH)
        .setSyncingTo(null)
        .build();
  }

  public static enum Health {
    NOT_CHECKED(-1),
    UNREACHABLE(0),
    UP(1);

    private final double id;

    private Health(double id) {
      this.id = id;
    }

    public double getId() {
      return id;
    }

    public static Health fromId(double id) throws IllegalArgumentException {
      for (Health value : Health.values()) {
        if (Double.compare(value.getId(), id) == 0) {
          return value;
        }
      }
      throw new IllegalArgumentException("There is no valid health element whose id is '"
          + id + "'");
    }
  }

  public static class Builder {

    private Health health;
    private Instant upSince;
    private Instant lastHeartbeat;
    private Instant lastHeartbeatRecv;
    private boolean authIssue;
    private ReplSetHeartbeatReply lastResponse;

    public Builder() {
    }

    public Builder(MemberHeartbeatData other) {
      this.health = other.health;
      this.upSince = other.upSince;
      this.lastHeartbeat = other.lastHeartbeat;
      this.lastHeartbeatRecv = other.lastHeartbeatRecv;
      this.authIssue = other.authIssue;
      this.lastResponse = other.lastResponse;
    }

    public Health getHealth() {
      return health;
    }

    public Builder setHealth(Health health) {
      this.health = health;
      return this;
    }

    public Instant getUpSince() {
      return upSince;
    }

    public Builder setUpSince(Instant upSince) {
      this.upSince = upSince;
      return this;
    }

    public Instant getLastHeartbeat() {
      return lastHeartbeat;
    }

    public Builder setLastHeartbeat(Instant lastHeartbeat) {
      this.lastHeartbeat = lastHeartbeat;
      return this;
    }

    public Instant getLastHeartbeatRecv() {
      return lastHeartbeatRecv;
    }

    public Builder setLastHeartbeatRecv(Instant lastHeartbeatRecv) {
      this.lastHeartbeatRecv = lastHeartbeatRecv;
      return this;
    }

    public boolean isAuthIssue() {
      return authIssue;
    }

    public Builder setAuthIssue(boolean authIssue) {
      this.authIssue = authIssue;
      return this;
    }

    public ReplSetHeartbeatReply getLastResponse() {
      return lastResponse;
    }

    public Builder setLastResponse(ReplSetHeartbeatReply lastResponse) {
      this.lastResponse = lastResponse;
      return this;
    }

    public MemberHeartbeatData build() {
      return new MemberHeartbeatData(health, upSince, lastHeartbeat, lastHeartbeatRecv,
          authIssue, lastResponse);
    }
  }
}
