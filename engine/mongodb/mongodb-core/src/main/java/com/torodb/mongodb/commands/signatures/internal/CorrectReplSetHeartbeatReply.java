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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.pojos.MemberState;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;

import javax.annotation.Nullable;

/**
 *
 */
class CorrectReplSetHeartbeatReply implements ReplSetHeartbeatReply {

  @Nullable
  private final BsonTimestamp electionTime;
  @Nullable
  private final Duration time;
  @Nullable
  private final OpTime appliedOptime;
  @Nullable
  private final OpTime durableOptime;
  @Nullable
  private final Boolean electable;
  @Nullable
  private final Boolean hasData;
  private final boolean mismatch;
  @Nullable
  private final Boolean isReplSet;
  private final boolean stateDisagreement;
  @Nullable
  private final MemberState state;
  private final long configVersion;
  @Nullable
  private final String setName;
  private final String hbmsg;
  @Nullable
  private final HostAndPort syncingTo;
  @Nullable
  private final ReplicaSetConfig config;
  private final OptionalInt primaryId;
  private final long term;

  public CorrectReplSetHeartbeatReply(BsonTimestamp electionTime,
      Duration time, OpTime appliedOptime,
      OpTime durableOptime, Boolean electable,
      Boolean hasData, boolean mismatch, Boolean isReplSet,
      boolean stateDisagreement, MemberState state,
      long configVersion, String setName, String hbmsg,
      HostAndPort syncingTo, ReplicaSetConfig config,
      OptionalInt primaryId, long term) {
    this.electionTime = electionTime;
    this.time = time;
    this.appliedOptime = appliedOptime;
    this.durableOptime = durableOptime;
    this.electable = electable;
    this.hasData = hasData;
    this.mismatch = mismatch;
    this.isReplSet = isReplSet;
    this.stateDisagreement = stateDisagreement;
    this.state = state;
    this.configVersion = configVersion;
    this.setName = setName;
    this.hbmsg = hbmsg;
    this.syncingTo = syncingTo;
    this.config = config;
    this.primaryId = primaryId;
    this.term = term;
  }

  @Override
  public ErrorCode getErrorCode() {
    return ErrorCode.OK;
  }

  @Override
  public Optional<String> getErrMsg() {
    return Optional.empty();
  }

  @Override
  public Optional<BsonTimestamp> getElectionTime() {
    return Optional.ofNullable(electionTime);
  }

  @Override
  public Optional<Duration> getTime() {
    return Optional.ofNullable(time);
  }

  @Override
  public Optional<OpTime> getAppliedOpTime() {
    return Optional.ofNullable(appliedOptime);
  }

  @Override
  public Optional<OpTime> getDurableOptime() {
    return Optional.ofNullable(durableOptime);
  }

  @Override
  public Optional<Boolean> getElectable() {
    return Optional.ofNullable(electable);
  }

  @Override
  public Optional<Boolean> getHasData() {
    return Optional.ofNullable(hasData);
  }

  @Override
  public boolean isMismatch() {
    return mismatch;
  }

  @Override
  public Optional<Boolean> getIsReplSet() {
    return Optional.ofNullable(isReplSet);
  }

  @Override
  public boolean isStateDisagreement() {
    return stateDisagreement;
  }

  @Override
  public Optional<MemberState> getState() {
    return Optional.ofNullable(state);
  }

  @Override
  public long getConfigVersion() {
    return configVersion;
  }

  @Override
  public Optional<String> getSetName() {
    return Optional.ofNullable(setName);
  }

  @Override
  public String getHbmsg() {
    return hbmsg;
  }

  @Override
  public Optional<HostAndPort> getSyncingTo() {
    return Optional.ofNullable(syncingTo);
  }

  @Override
  public Optional<ReplicaSetConfig> getConfig() {
    return Optional.ofNullable(config);
  }

  @Override
  public OptionalInt getPrimaryId() {
    return primaryId;
  }

  @Override
  public long getTerm() {
    return term;
  }

}
