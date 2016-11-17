/*
 * ToroDB - ToroDB: MongoDB Core
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.mongodb.commands.signatures.internal;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.torodb.mongodb.commands.pojos.MemberState;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.google.common.net.HostAndPort;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class ReplSetHeartbeatReplyBuilder {

    private Optional<BsonTimestamp> electionTime;
    private Optional<Duration> time;
    private Optional<OpTime> durableOpTime;
    private Optional<OpTime> appliedOpTime;
    private Optional<Boolean> electable;
    private Optional<Boolean> hasData = Optional.empty();
    private boolean mismatch;
    private Optional<Boolean> isReplSet;
    private boolean stateDisagreement;
    private Optional<MemberState> state;
    private long configVersion;
    private Optional<String> setName;
    @Nonnull
    private String hbmsg;
    private Optional<HostAndPort> syncingTo;
    private Optional<ReplicaSetConfig> config;
    private OptionalInt primaryId;
    private long term;

    public ReplSetHeartbeatReplyBuilder() {
        this.hbmsg = "";
    }

    public ReplSetHeartbeatReplyBuilder(ReplSetHeartbeatReply other) {
        this.electionTime = other.getElectionTime();
        this.time = other.getTime();
        this.appliedOpTime = other.getAppliedOpTime();
        this.electable = other.getElectable();
        this.hasData = other.getHasData();
        this.mismatch = other.isMismatch();
        this.isReplSet = other.getIsReplSet();
        this.stateDisagreement = other.isStateDisagreement();
        this.state = other.getState();
        this.configVersion = other.getConfigVersion();
        this.setName = other.getSetName();
        this.hbmsg = other.getHbmsg();
        this.syncingTo = other.getSyncingTo();
        this.config = other.getConfig();
        this.primaryId = other.getPrimaryId();
        this.term = other.getTerm();
    }

    public ReplSetHeartbeatReplyBuilder setElectionTime(@Nullable BsonTimestamp electionTime) {
        this.electionTime = Optional.ofNullable(electionTime);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setElectionTime(Optional<BsonTimestamp> electionTime) {
        this.electionTime = electionTime;
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setTime(@Nullable Duration time) {
        this.time = Optional.ofNullable(time);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setTerm(long term) {
        this.term = term;
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setDurableOpTime(@Nullable OpTime durableOpTime) {
        this.durableOpTime = Optional.ofNullable(durableOpTime);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setAppliedOpTime(@Nullable OpTime appliedOpTime) {
        this.appliedOpTime = Optional.ofNullable(appliedOpTime);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setAppliedOpTime(Optional<OpTime> appliedOpTime) {
        this.appliedOpTime = appliedOpTime;
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setElectable(boolean electable) {
        this.electable = Optional.of(electable);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setHasData(@Nullable Boolean hasData) {
        this.hasData = Optional.ofNullable(hasData);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setMismatch(boolean mismatch) {
        this.mismatch = mismatch;
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setIsReplSet(boolean isReplSet) {
        this.isReplSet = Optional.of(isReplSet);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setStateDisagreement(boolean stateDisagreement) {
        this.stateDisagreement = stateDisagreement;
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setState(@Nullable MemberState state) {
        this.state = Optional.ofNullable(state);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setConfigVersion(long configVersion) {
        this.configVersion = configVersion;
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setSetName(@Nullable String setName) {
        this.setName = Optional.ofNullable(setName);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setSetName(Optional<String> setName) {
        this.setName = setName;
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setHbmsg(@Nonnull String hbmsg) {
        this.hbmsg = hbmsg;
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setSyncingTo(@Nullable HostAndPort syncingTo) {
        this.syncingTo = Optional.ofNullable(syncingTo);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setConfig(@Nullable ReplicaSetConfig config) {
        this.config = Optional.ofNullable(config);
        return this;
    }

    public ReplSetHeartbeatReplyBuilder setPrimaryId(int primaryId) {
        this.primaryId = OptionalInt.of(primaryId);
        return this;
    }

    Optional<OpTime> getAppliedOpTime() {
        return appliedOpTime;
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
