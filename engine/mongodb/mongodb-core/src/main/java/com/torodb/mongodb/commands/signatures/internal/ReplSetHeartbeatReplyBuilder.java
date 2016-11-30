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

package com.torodb.mongodb.commands.signatures.internal;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.pojos.MemberState;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ReplSetHeartbeatReplyBuilder {

  @Nullable
  private BsonTimestamp electionTime;
  @Nullable
  private Duration time;
  @Nullable
  private OpTime durableOpTime;
  @Nullable
  private OpTime appliedOpTime;
  @Nullable
  private Boolean electable;
  @Nullable
  private Boolean hasData = null;
  private boolean mismatch;
  @Nullable
  private Boolean isReplSet;
  private boolean stateDisagreement;
  @Nullable
  private MemberState state;
  private long configVersion;
  @Nullable
  private String setName;
  @Nonnull
  private String hbmsg;
  @Nullable
  private HostAndPort syncingTo;
  @Nullable
  private ReplicaSetConfig config;
  private OptionalInt primaryId;
  private long term;

  public ReplSetHeartbeatReplyBuilder() {
    this.hbmsg = "";
  }

  public ReplSetHeartbeatReplyBuilder(ReplSetHeartbeatReply other,
      ReplSetHeartbeatReply lastResponse) {
    this.electionTime = other.getElectionTime()
        .orElseGet(() -> lastResponse.getElectionTime().orElse(null));
    this.time = other.getTime().orElse(null);
    this.appliedOpTime = other.getAppliedOpTime()
        .orElseGet(() -> lastResponse.getAppliedOpTime().orElse(null));
    this.electable = other.getElectable().orElse(null);
    this.hasData = other.getHasData().orElse(null);
    this.mismatch = other.isMismatch();
    this.isReplSet = other.getIsReplSet().orElse(null);
    this.stateDisagreement = other.isStateDisagreement();
    this.state = other.getState()
        .orElseGet(() -> lastResponse.getState().orElse(null));
    this.configVersion = other.getConfigVersion();
    this.setName = other.getSetName().orElse(null);
    this.hbmsg = other.getHbmsg();
    this.syncingTo = other.getSyncingTo().orElse(null);
    this.config = other.getConfig().orElse(null);
    this.primaryId = other.getPrimaryId();
    this.term = other.getTerm();
  }

  public ReplSetHeartbeatReplyBuilder setElectionTime(@Nullable BsonTimestamp electionTime) {
    this.electionTime = electionTime;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setElectionTime(Optional<BsonTimestamp> electionTime) {
    this.electionTime = electionTime.orElse(null);
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setTime(@Nullable Duration time) {
    this.time = time;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setTerm(long term) {
    this.term = term;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setDurableOpTime(@Nullable OpTime durableOpTime) {
    this.durableOpTime = durableOpTime;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setAppliedOpTime(@Nullable OpTime appliedOpTime) {
    this.appliedOpTime = appliedOpTime;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setAppliedOpTime(Optional<OpTime> appliedOpTime) {
    this.appliedOpTime = appliedOpTime.orElse(null);
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setElectable(boolean electable) {
    this.electable = electable;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setHasData(@Nullable Boolean hasData) {
    this.hasData = hasData;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setMismatch(boolean mismatch) {
    this.mismatch = mismatch;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setIsReplSet(boolean isReplSet) {
    this.isReplSet = isReplSet;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setStateDisagreement(boolean stateDisagreement) {
    this.stateDisagreement = stateDisagreement;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setState(@Nullable MemberState state) {
    this.state = state;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setConfigVersion(long configVersion) {
    this.configVersion = configVersion;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setSetName(@Nullable String setName) {
    this.setName = setName;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setSetName(Optional<String> setName) {
    this.setName = setName.orElse(null);
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setHbmsg(@Nonnull String hbmsg) {
    this.hbmsg = hbmsg;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setSyncingTo(@Nullable HostAndPort syncingTo) {
    this.syncingTo = syncingTo;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setConfig(@Nullable ReplicaSetConfig config) {
    this.config = config;
    return this;
  }

  public ReplSetHeartbeatReplyBuilder setPrimaryId(int primaryId) {
    this.primaryId = OptionalInt.of(primaryId);
    return this;
  }

  Optional<OpTime> getAppliedOpTime() {
    return Optional.ofNullable(appliedOpTime);
  }

  public ReplSetHeartbeatReply build() {
    return new CorrectReplSetHeartbeatReply(
        electionTime,
        time,
        appliedOpTime,
        durableOpTime,
        electable,
        hasData,
        mismatch,
        isReplSet,
        stateDisagreement,
        state,
        configVersion,
        setName,
        hbmsg,
        syncingTo,
        config,
        primaryId,
        term);
  }
}
