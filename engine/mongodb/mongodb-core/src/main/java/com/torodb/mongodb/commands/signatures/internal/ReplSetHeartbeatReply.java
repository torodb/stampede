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

/**
 *
 */
public interface ReplSetHeartbeatReply {

  public ErrorCode getErrorCode();

  public Optional<String> getErrMsg();

  public Optional<BsonTimestamp> getElectionTime();

  public Optional<Duration> getTime();

  public Optional<OpTime> getAppliedOpTime();

  public Optional<OpTime> getDurableOptime();

  public Optional<Boolean> getElectable();

  public Optional<Boolean> getHasData();

  public boolean isMismatch();

  public boolean isStateDisagreement();

  public Optional<MemberState> getState();

  public long getConfigVersion();

  public Optional<String> getSetName();

  public String getHbmsg();

  public Optional<HostAndPort> getSyncingTo();

  public Optional<ReplicaSetConfig> getConfig();

  public OptionalInt getPrimaryId();

  public long getTerm();

  /**
   * Returns an optional that indicates if the node is on a replica set or on a master slave
   * replication.
   *
   * @return an optional whose value is true if the node is on a replica set, false if it is on
   *         master slave or empty if it is unknown
   */
  public Optional<Boolean> getIsReplSet();

}
