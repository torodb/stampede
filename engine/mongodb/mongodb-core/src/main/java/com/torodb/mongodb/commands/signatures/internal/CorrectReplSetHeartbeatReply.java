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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.torodb.mongodb.commands.pojos.MemberState;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.google.common.net.HostAndPort;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;

/**
 *
 */
class CorrectReplSetHeartbeatReply implements ReplSetHeartbeatReply {
    private final Optional<BsonTimestamp> electionTime;
    private final Optional<Duration> time;
    private final Optional<OpTime> appliedOptime;
    private final Optional<OpTime> durableOptime;
    private final Optional<Boolean> electable;
    private final Optional<Boolean> hasData;
    private final boolean mismatch;
    private final Optional<Boolean> isReplSet;
    private final boolean stateDisagreement;
    private final Optional<MemberState> state;
    private final long configVersion;
    private final Optional<String> setName;
    private final String hbmsg;
    private final Optional<HostAndPort> syncingTo;
    private final Optional<ReplicaSetConfig> config;
    private final OptionalInt primaryId;
    private final long term;

    public CorrectReplSetHeartbeatReply(Optional<BsonTimestamp> electionTime,
            Optional<Duration> time, Optional<OpTime> appliedOptime,
            Optional<OpTime> durableOptime, Optional<Boolean> electable,
            Optional<Boolean> hasData, boolean mismatch, Optional<Boolean> isReplSet,
            boolean stateDisagreement, Optional<MemberState> state,
            long configVersion, Optional<String> setName, String hbmsg,
            Optional<HostAndPort> syncingTo, Optional<ReplicaSetConfig> config,
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
        return electionTime;
    }

    @Override
    public Optional<Duration> getTime() {
        return time;
    }

    @Override
    public Optional<OpTime> getAppliedOpTime() {
        return appliedOptime;
    }

    @Override
    public Optional<OpTime> getDurableOptime() {
        return durableOptime;
    }

    @Override
    public Optional<Boolean> getElectable() {
        return electable;
    }

    @Override
    public Optional<Boolean> getHasData() {
        return hasData;
    }

    @Override
    public boolean isMismatch() {
        return mismatch;
    }

    @Override
    public Optional<Boolean> getIsReplSet() {
        return isReplSet;
    }

    @Override
    public boolean isStateDisagreement() {
        return stateDisagreement;
    }

    @Override
    public Optional<MemberState> getState() {
        return state;
    }

    @Override
    public long getConfigVersion() {
        return configVersion;
    }

    @Override
    public Optional<String> getSetName() {
        return setName;
    }

    @Override
    public String getHbmsg() {
        return hbmsg;
    }

    @Override
    public Optional<HostAndPort> getSyncingTo() {
        return syncingTo;
    }

    @Override
    public Optional<ReplicaSetConfig> getConfig() {
        return config;
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
