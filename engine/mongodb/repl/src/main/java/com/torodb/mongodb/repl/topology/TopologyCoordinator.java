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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.client.core.MongoConnection.RemoteCommandResponse;
import com.eightkdata.mongowp.exceptions.HostUnreachableException;
import com.eightkdata.mongowp.exceptions.InvalidOptionsException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NodeNotFoundException;
import com.eightkdata.mongowp.exceptions.ShutdownInProgressException;
import com.eightkdata.mongowp.exceptions.UnauthorizedException;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.UnsignedInteger;
import com.torodb.mongodb.commands.pojos.MemberConfig;
import com.torodb.mongodb.commands.pojos.MemberHeartbeatData;
import com.torodb.mongodb.commands.pojos.MemberHeartbeatData.Health;
import com.torodb.mongodb.commands.pojos.MemberState;
import com.torodb.mongodb.commands.pojos.ReplSetProtocolVersion;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatCommand.ReplSetHeartbeatArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetSyncFromCommand.ReplSetSyncFromReply;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Stream;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Objects of this class are responsible for managing the topology of the cluster when the node is
 * not member of the replica set.
 *
 * Methods of this class should be non-blocking.
 */
@NotThreadSafe
@SuppressWarnings("checkstyle:MemberName")
class TopologyCoordinator {

  private static final Logger LOGGER = LogManager.getLogger(TopologyCoordinator.class);

  /**
   * Maximum number of retries for a failed heartbeat.
   */
  private static final int MAX_HEARTBEAT_RETRIES = 2;

  /**
   * Interval between the time the last heartbeat from a node was received successfully, or the time
   * when we gave up retrying, and when the next heartbeat should be sent to a target.
   */
  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(2);

  /**
   * the index of the member we currently believe is primary, if one exists, otherwise -1
   */
  private int _currentPrimaryIndex;

  /**
   * the hostandport we are currently syncing from or {@link Optional#empty()} if no sync source (we
   * are cannot connect to anyone yet)
   */
  @Nonnull
  private Optional<HostAndPort> _syncSource;
  /**
   * These members are not chosen as sync sources for a period of time, due to connection issues
   * with them
   */
  private final Map<HostAndPort, Instant> _syncSourceBlacklist;
  /**
   * The next sync source to be chosen, requested via a replSetSyncFrom command
   */
  private int _forceSyncSourceIndex;
  /**
   * How far this node must fall behind before considering switching sync sources
   */
  private final int _maxSyncSourceLagSecs;

  /**
   * The current config, including a vector of MemberConfigs.
   */
  private ReplicaSetConfig _rsConfig;

  /**
   * heartbeat data for each member. It is guaranteed that this vector will be maintained in the
   * same order as the MemberConfigs in _currentConfig, therefore the member config index can be
   * used to index into this vector as well.
   */
  private List<MemberHeartbeatData> _hbdata;

  /**
   * Ping stats for each member by HostAndPort;
   */
  private final Map<HostAndPort, PingStats> _pings;
  private final long slaveDelaySecs;
  private final Set<VersionChangeListener> versionListeners = Collections.newSetFromMap(
      new WeakHashMap<>());

  /**
   *
   * @param maxSyncSourceLagSecs
   * @param slaveDelay           our delay. It is rounded to seconds and must be non negative.
   */
  public TopologyCoordinator(Duration maxSyncSourceLag, Duration slaveDelay) {
    this._currentPrimaryIndex = -1;
    this._syncSource = Optional.empty();
    this._syncSourceBlacklist = new HashMap<>();
    this._forceSyncSourceIndex = -1;
    Preconditions.checkArgument(!maxSyncSourceLag.isNegative(),
        "Negative max sync source lag is not accepted");
    this._maxSyncSourceLagSecs = (int) maxSyncSourceLag.getSeconds();
    this._pings = new HashMap<>();
    this.slaveDelaySecs = slaveDelay.getSeconds();
    Preconditions.checkArgument(slaveDelaySecs >= 0, "Slave delay must be "
        + "non negative, but %s was found", slaveDelay);
  }

  @Nonnull
  private PingStats getPingOrDefault(HostAndPort hostAndPort) {
    PingStats result = _pings.get(hostAndPort);
    if (result == null) {
      result = new PingStats();
      _pings.put(hostAndPort, result);
    }
    return result;
  }

  int getCurrentPrimaryIndex() {
    return _currentPrimaryIndex;
  }

  public ReplicaSetConfig getRsConfig() {
    return _rsConfig;
  }

  public void addVersionChangeListener(VersionChangeListener listener) {
    versionListeners.add(listener);
  }

  /**
   * Returns the address of the current sync source, or an empty HostAndPort if there is no current
   * sync source.
   */
  @Nonnull
  Optional<HostAndPort> getSyncSourceAddress() {
    return _syncSource;
  }

  /**
   * Retrieves a vector of HostAndPorts containing all nodes that are neither DOWN.
   */
  List<HostAndPort> getMaybeUpHostAndPorts() {
    List<HostAndPort> upHosts = new ArrayList<>(_hbdata.size());
    for (int i = 0; i < _hbdata.size(); i++) {
      MemberHeartbeatData it = _hbdata.get(i);
      if (it.maybeUp()) {
        continue;  // skip DOWN nodes
      }
      upHosts.add(_rsConfig.getMembers().get(i).getHostAndPort());
    }
    return upHosts;
  }

  /**
   * Sets the index into the config used when we next choose a sync source
   */
  void setForceSyncSourceIndex(int index) {
    assert _forceSyncSourceIndex < _rsConfig.getMembers().size();
    _forceSyncSourceIndex = index;
  }

  /**
   * Chooses and sets a new sync source, based on our current knowledge of the world.
   *
   * @return the new sync source or {@link Optional#empty()} if we cannot calculate a new sync
   *         source yet
   */
  @Nonnull
  Optional<HostAndPort> chooseNewSyncSource(Instant now, Optional<OpTime> lastOpApplied) {
    // if we have a target we've requested to sync from, use it
    if (_forceSyncSourceIndex != -1) {
      assert _forceSyncSourceIndex < _rsConfig.getMembers().size();
      HostAndPort syncSource = _rsConfig.getMembers().get(_forceSyncSourceIndex).getHostAndPort();
      _syncSource = Optional.of(syncSource);
      _forceSyncSourceIndex = -1;
      String msg = "syncing from: " + syncSource + " by request";
      LOGGER.info(msg);
      return _syncSource;
    }

    // wait for 2N pings before choosing a sync target
    if (_hbdata == null) { //we dont have a repl config yet
      assert _rsConfig == null;
      return Optional.empty();
    }
    int needMorePings = _hbdata.size() * 2 - getTotalPings();

    if (needMorePings > 0) {
      LOGGER.info("Waiting for {}  pings from other members before syncing", needMorePings);
      _syncSource = Optional.empty();
      return _syncSource;
    }

    // If we are only allowed to sync from the primary, set that
    if (!_rsConfig.isChainingAllowed()) {
      if (_currentPrimaryIndex == -1) {
        LOGGER.warn("Cannot select sync source because chaining is not allowed and primary "
            + "is unknown/down");
        _syncSource = Optional.empty();
        return _syncSource;
      } else if (isBlacklistedMember(getCurrentPrimaryMember(), now)) {
        LOGGER.warn("Cannot select sync source because chaining is not allowed and "
            + "primary is not currently accepting our updates");
        _syncSource = Optional.empty();
        return _syncSource;
      } else {
        HostAndPort syncSource = _rsConfig.getMembers().get(_currentPrimaryIndex).getHostAndPort();
        _syncSource = Optional.of(syncSource);
        String msg = "syncing from primary: " + syncSource;
        LOGGER.info(msg);
        return _syncSource;
      }
    }

    // find the member with the lowest ping time that is ahead of me
    // Find primary's oplog time. Reject sync candidates that are more than
    // maxSyncSourceLagSecs seconds behind.
    OpTime primaryOpTime;
    if (_currentPrimaryIndex != -1) {
      primaryOpTime = _hbdata.get(_currentPrimaryIndex).getOpTime();
      assert primaryOpTime != null;
    } else {
      // choose a time that will exclude no candidates, since we don't see a primary
      primaryOpTime = OpTime.ofSeconds(_maxSyncSourceLagSecs);
    }

    if (primaryOpTime.getSecs() < _maxSyncSourceLagSecs) {
      // erh - I think this means there was just a new election
      // and we don't yet know the new primary's optime
      primaryOpTime = OpTime.ofSeconds(_maxSyncSourceLagSecs);
    }

    OpTime oldestSyncOpTime = OpTime.ofSeconds(primaryOpTime.getSecs() - _maxSyncSourceLagSecs);

    Optional<MemberConfig> newSyncSourceMember = lookForSyncSource(now, lastOpApplied, true,
        oldestSyncOpTime);
    if (!newSyncSourceMember.isPresent()) {
      newSyncSourceMember = lookForSyncSource(now, lastOpApplied, false, oldestSyncOpTime);
    }

    if (!newSyncSourceMember.isPresent()) {
      // Did not find any members to sync from
      String msg = "could not find member to sync from";
      // Only log when we had a valid sync source before
      if (_syncSource.isPresent()) {
        LOGGER.info(msg);
      }

      _syncSource = Optional.empty();
      return _syncSource;
    } else {
      _syncSource = Optional.of(newSyncSourceMember.get().getHostAndPort());
      LOGGER.info("syncing from: {}", _syncSource.get());
      return _syncSource;
    }
  }

  private MemberConfig getMemberConfig(MemberHeartbeatData hbData) {
    int indexOf = _hbdata.indexOf(hbData);
    Preconditions.checkArgument(indexOf >= 0, "Unknown hb data");
    return _rsConfig.getMembers().get(indexOf);
  }

  /**
   * Looks for an optimal sync source to replicate from.
   *
   * The first attempt, we ignore those nodes with slave delay higher than our own, hidden nodes,
   * and nodes that are excessively lagged. The second attempt includes such nodes, in case those
   * are the only ones we can reach. This loop attempts to set 'closestIndex'.
   *
   * @param now              the current time
   * @param lastOpAppliedOp  the last OpTime this node has apply
   * @param onlyOptimal      if true, slaves with more delay than ourselve, hidden nodes or
   *                         excessively lagged nodes are ignored
   * @param oldestSyncOpTime the oldest optime considered not excessively lagged. Only used if
   *                         onlyOptimal is true.
   * @return the new optimal sync source, which is not {@link Optional#isPresent() present} if no
   *         one can be chosen
   */
  private Optional<MemberConfig> lookForSyncSource(Instant now, Optional<OpTime> lastOpAppliedOp,
      boolean onlyOptimal, OpTime oldestSyncOpTime) {
    OpTime lastOpApplied = lastOpAppliedOp.orElse(OpTime.EPOCH);
    Stream<MemberHeartbeatData> hbCandidateStream = _hbdata.stream()
        // candidate must be up to be considered
        .filter(MemberHeartbeatData::isUp)
        // candidate must be PRIMARY or SECONDARY state to be considered.
        .filter(hbData -> hbData.getState().isReadable())
        // only consider candidates that are ahead of where we are
        .filter(hbData ->
            hbData.getOpTime().isAfter(lastOpApplied)
        );
    if (onlyOptimal) {
      hbCandidateStream = hbCandidateStream
          // omit candidates that are excessively behind
          .filter(hbData -> hbData.getOpTime().isEqualOrAfter(oldestSyncOpTime));
    }
    Stream<MemberConfig> mcCandidateStream = hbCandidateStream.map(this::getMemberConfig)
        // omit candidates that are blacklisted
        .filter(mc -> !isBlacklistedMember(mc, now));
    if (onlyOptimal) {
      mcCandidateStream = mcCandidateStream
          // only candidates that are not hidden
          .filter(mc -> !mc.isHidden())
          // only candidates whose slave delay is shorter than ours
          .filter(mc -> mc.getSlaveDelay() < slaveDelaySecs);
    }

    //If there are several candidates, the one whose ping is lower is returned
    return mcCandidateStream.reduce((MemberConfig cand1, MemberConfig cand2) -> {
      long ping1 = getPing(cand1.getHostAndPort());
      long ping2 = getPing(cand2.getHostAndPort());
      if (ping1 < ping2) {
        return cand1;
      }
      return cand2;
    });
  }

  /**
   * Suppresses selecting "host" as sync source until "until".
   */
  void blacklistSyncSource(HostAndPort host, Instant until) {
    LOGGER.debug("blacklisting {} until {}", host, until);
    _syncSourceBlacklist.put(host, until);
  }

  /**
   * Removes a single entry "host" from the list of potential sync sources which we have
   * blacklisted, if it is supposed to be unblacklisted by "now".
   *
   * @param host the host that is wanted to be removed from the black list
   * @param now  the node will be removed from the black list if it is supposed to be unblacklisted
   *             by 'now.
   */
  void unblacklistSyncSource(HostAndPort host, Instant now) {
    Instant oldInstant = _syncSourceBlacklist.get(host);
    if (oldInstant != null && !now.isBefore(oldInstant)) {
      LOGGER.debug("unblacklisting {}", host);
      _syncSourceBlacklist.remove(host);
    }
  }

  /**
   * Clears the list of potential sync sources we have blacklisted.
   */
  void clearSyncSourceBlacklist() {
    _syncSourceBlacklist.clear();
  }

  /**
   * Determines if a new sync source should be chosen, if a better candidate sync source is
   * available.
   *
   * It returns true if there exists a viable sync source member other than our current source,
   * whose oplog has reached an optime greater than the max sync source lag later than current
   * source's. It can return true in other scenarios (like if {@link #setForceSyncSourceIndex(int) }
   * has been called or if we don't have a current sync source.
   *
   * @param now is used to skip over currently blacklisted sync sources.
   * @return
   */
  boolean shouldChangeSyncSource(HostAndPort currentSource, Instant now) {
    // Methodology:
    // If there exists a viable sync source member other than currentSource, whose oplog has
    // reached an optime greater than _maxSyncSourceLagSecs later than currentSource's, return
    // true.

    // If the user requested a sync source change, return true.
    if (_forceSyncSourceIndex != -1) {
      return true;
    }

    OptionalInt currentMemberIndex = _rsConfig.findMemberIndexByHostAndPort(currentSource);
    if (!currentMemberIndex.isPresent()) {
      return true;
    }

    assert _hbdata.get(currentMemberIndex.getAsInt()) != null;
    OpTime currentOpTime = _hbdata.get(currentMemberIndex.getAsInt()).getOpTime();
    if (currentOpTime == null) {
      // Haven't received a heartbeat from the sync source yet, so can't tell if we should
      // change.
      return false;
    }
    long currentSecs = currentOpTime.getSecs();
    long goalSecs = currentSecs + _maxSyncSourceLagSecs;

    for (int i = 0; i < _hbdata.size(); i++) {
      MemberHeartbeatData it = _hbdata.get(i);

      MemberConfig candidateConfig = _rsConfig.getMembers().get(i);
      OpTime itOpTime = it.getOpTime();

      if (itOpTime != null && it.isUp()
          && it.getState().isReadable() && !isBlacklistedMember(candidateConfig, now)
          && goalSecs < itOpTime.getSecs()) {
        LOGGER.info("changing sync target because current sync target's most recent OpTime "
            + "is {}  which is more than {} seconds behind member {} whose most recent "
            + "OpTime is {} ", currentOpTime, _maxSyncSourceLagSecs,
            candidateConfig.getHostAndPort(), itOpTime);
        return true;
      }
    }
    return false;
  }

  ReplSetSyncFromReply executeReplSetSyncFrom(ErrorCode status, HostAndPort target,
      OpTime lastOpApplied)
      throws MongoException {
    if (status == ErrorCode.CALLBACK_CANCELED) {
      throw new ShutdownInProgressException("replication system is shutting down");
    }

    final HostAndPort syncFromRequested = target;

    MemberConfig targetConfig = null;
    int targetIndex;
    for (targetIndex = 0; targetIndex < _rsConfig.getMembers().size(); targetIndex++) {
      MemberConfig it = _rsConfig.getMembers().get(targetIndex);

      if (it.getHostAndPort().equals(target)) {
        targetConfig = it;
        break;
      }
    }
    if (targetConfig == null) {
      throw new NodeNotFoundException("Could not find member \"" + target + "\" in replica set");
    }
    if (targetConfig.isArbiter()) {
      throw new InvalidOptionsException("Cannot sync from \"" + target
          + "\" because it is an arbiter");
    }

    String warning = null;

    MemberHeartbeatData hbdata = _hbdata.get(targetIndex);
    if (hbdata.isAuthIssue()) {
      throw new UnauthorizedException("not authorized to communicate with " + target);
    }
    if (hbdata.getHealth() == Health.UNREACHABLE) {
      throw new HostUnreachableException("I cannot reach the requested member: " + target);
    }
    assert hbdata.getOpTime() != null;
    if (hbdata.getOpTime().getSecs() + 10 < lastOpApplied.getSecs()) {
      LOGGER.warn("attempting to sync from {}, but its latest opTime is {} and ours is {} "
          + "so this may not work", target, hbdata.getOpTime().getSecs(), lastOpApplied.getSecs());

      warning = "requested member \"" + target + "\" is more than 10 seconds behind us";
    }

    HostAndPort prevSyncSource = getSyncSourceAddress().orElse(null);

    setForceSyncSourceIndex(targetIndex);

    return new ReplSetSyncFromReply(prevSyncSource, syncFromRequested, warning);
  }

  /**
   * Updates the topology coordinator's notion of the replica set configuration.
   *
   * @param newConfig the new configuration. It should be not null except for testing purpose.
   * @param now
   */
  void updateConfig(ReplicaSetConfig newConfig, Instant now) {
    final ReplicaSetConfig oldConfig = _rsConfig;
    updateHeartbeatDataForReconfig(newConfig, now);
    _rsConfig = newConfig;
    _forceSyncSourceIndex = -1;

    _currentPrimaryIndex = -1;  // force secondaries to re-detect who the primary is

    versionListeners.forEach(listener -> listener.onVersionChange(this, oldConfig));
  }

  /**
   * Updates {@link #_hbdata} based on the newConfig, ensuring that every member in the newConfig
   * has an entry in _hbdata.
   * <p>
   * If any nodes in the newConfig are also present in {@link #_currentConfig}, copies their
   * heartbeat info into the corresponding entry in the updated _hbdata vector.
   */
  private void updateHeartbeatDataForReconfig(ReplicaSetConfig newConfig, Instant now) {
    if (newConfig == null) {
      return;
    }

    List<MemberHeartbeatData> oldHeartbeats = _hbdata;
    _hbdata = new ArrayList<>(newConfig.getMembers().size());

    for (int index = 0; index < newConfig.getMembers().size(); index++) {
      MemberConfig newMemberConfig = newConfig.getMembers().get(index);
      MemberHeartbeatData newHeartbeatData = new MemberHeartbeatData();

      if (_rsConfig != null) {
        for (int oldIndex = 0; oldIndex < _rsConfig.getMembers().size(); oldIndex++) {
          MemberConfig oldMemberConfig = _rsConfig.getMembers().get(oldIndex);
          if (oldMemberConfig.getId() == newMemberConfig.getId()
              && oldMemberConfig.getHostAndPort().equals(newMemberConfig.getHostAndPort())) {
            // This member existed in the old config with the same member ID and
            // HostAndPort, so copy its heartbeat data over.
            newHeartbeatData = oldHeartbeats.get(oldIndex);
            break;
          }
        }
      }
      _hbdata.add(newHeartbeatData);
    }
  }

  /**
   * Prepares a heartbeat request appropriate for sending to "target", assuming the current time is
   * "now".
   * <p>
   * The returned pair contains proper arguments for a replSetHeartbeat command, and an amount of
   * time to wait for the response.
   * <p>
   * This call should be paired (with intervening network communication) with a call to
   * processHeartbeatResponse for the same "target".
   *
   * @param now        our current time
   * @param ourSetName is used as the name for our replica set if the topology coordinator does not
   *                   have a valid configuration installed.
   * @param host       the target of the request to be created
   */
  RemoteCommandRequest<ReplSetHeartbeatArgument> prepareHeartbeatRequest(
      Instant now, String ourSetName, HostAndPort target) {
    PingStats hbStats = getPingOrDefault(target);

    Duration alreadyElapsed;
    if (hbStats.getLastHeartbeatStartDate() != null) {
      alreadyElapsed = Duration.between(hbStats.getLastHeartbeatStartDate(), now);
    } else {
      alreadyElapsed = Duration.between(Instant.EPOCH, now);
    }
    if (_rsConfig == null || (hbStats.getNumFailuresSinceLastStart() > MAX_HEARTBEAT_RETRIES)
        || (alreadyElapsed.toMillis() >= _rsConfig.getHeartbeatTimeoutPeriod())) {
      // This is either the first request ever for "target", or the heartbeat timeout has
      // passed, so we're starting a "new" heartbeat.
      hbStats.start(now);
      alreadyElapsed = Duration.ZERO;
    }

    ReplSetHeartbeatArgument.Builder hbArgs = new ReplSetHeartbeatArgument.Builder(
        ReplSetProtocolVersion.V1)
        .setCheckEmpty(false);
    if (_rsConfig != null) {
      hbArgs.setSetName(_rsConfig.getReplSetName());
      hbArgs.setConfigVersion(_rsConfig.getConfigVersion());
    } else {
      hbArgs.setSetName(ourSetName);
      hbArgs.setConfigVersion(-2);
    }

    final Duration timeoutPeriod = _rsConfig != null 
        ? Duration.ofMillis(_rsConfig.getHeartbeatTimeoutPeriod()) :
        HEARTBEAT_INTERVAL;
    Duration timeout = timeoutPeriod.minus(alreadyElapsed);

    return new RemoteCommandRequest<>(target, "admin", hbArgs.build(), timeout);
  }

  /**
   * Processes a heartbeat response from "target" that arrived around "now", having spent
   * "networkRoundTripTime" millis on the network.
   * <p>
   * Updates internal topology coordinator state, and returns instructions about what action to take
   * next.
   * <p>
   * If the next action is {@link HeartbeatResponseAction#makeNoAction() "NoAction"} then nothing
   * has to be done.
   * <p>
   * If the next action indicates {@link HeartbeatResponseAction#makeReconfigAction() "Reconfig"},
   * the caller should verify the configuration in hbResponse is acceptable, perform any other
   * reconfiguration actions it must, and call
   * {@link #updateConfig(
   * com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig,
   * java.time.Instant, com.eightkdata.mongowp.OpTime) updateConfig}
   * with the appropiate arguments.
   * <p>
   * This call should be paired (with intervening network communication) with a call to
   * prepareHeartbeatRequest for the same "target".
   *
   * @param now                  the aproximated time when the response has been recived
   * @param networkRoundTripTime the time spent on network
   * @param target               the host that send the respond
   * @param hbResponse
   */
  HeartbeatResponseAction processHeartbeatResponse(
      Instant now,
      Duration networkRoundTripTime,
      HostAndPort target,
      RemoteCommandResponse<ReplSetHeartbeatReply> hbResponse) {

    PingStats hbStats = getPingOrDefault(target);
    Preconditions.checkState(hbStats.getLastHeartbeatStartDate() != null, "It seems that a hb "
        + "response has been recived before it has been prepared");
    if (!hbResponse.isOk()) {
      hbStats.miss();
    } else {
      hbStats.hit(networkRoundTripTime);
    }

    boolean isUnauthorized = (hbResponse.getErrorCode() == ErrorCode.UNAUTHORIZED) || (hbResponse
        .getErrorCode() == ErrorCode.AUTHENTICATION_FAILED);

    Duration alreadyElapsed = Duration.between(hbStats.getLastHeartbeatStartDate(), now);
    Duration nextHeartbeatDelay;
    // determine next start time
    if (_rsConfig != null && (hbStats.getNumFailuresSinceLastStart() <= MAX_HEARTBEAT_RETRIES)
        && (alreadyElapsed.toMillis() < _rsConfig.getHeartbeatTimeoutPeriod())) {
      if (isUnauthorized) {
        nextHeartbeatDelay = HEARTBEAT_INTERVAL;
      } else {
        nextHeartbeatDelay = Duration.ZERO;
      }
    } else {
      nextHeartbeatDelay = HEARTBEAT_INTERVAL;
    }

    Optional<ReplSetHeartbeatReply> commandReply = hbResponse.getCommandReply();
    if (hbResponse.isOk() && commandReply.get().getConfig().isPresent()) {
      long currentConfigVersion = _rsConfig != null ? _rsConfig.getConfigVersion() : -2;
      ReplicaSetConfig newConfig = commandReply.get().getConfig().get();
      assert newConfig != null;
      if (newConfig.getConfigVersion() > currentConfigVersion) {
        HeartbeatResponseAction nextAction = HeartbeatResponseAction.makeReconfigAction()
            .setNextHeartbeatDelay(nextHeartbeatDelay);
        return nextAction;
      } else {
        // Could be we got the newer version before we got the response, or the
        // target erroneously sent us one, even through it isn't newer.
        if (newConfig.getConfigVersion() < currentConfigVersion) {
          LOGGER.debug("Config version from heartbeat was older than ours.");
          LOGGER.trace("Current config: {}. Config from heartbeat: {}", _rsConfig, newConfig);
        } else {
          LOGGER.trace("Config from heartbeat response was same as ours.");
        }
      }
    }

    // Check if the heartbeat target is in our config.  If it isn't, there's nothing left to do,
    // so return early.
    if (_rsConfig == null) {
      HeartbeatResponseAction nextAction = HeartbeatResponseAction.makeNoAction();
      nextAction.setNextHeartbeatDelay(nextHeartbeatDelay);
      return nextAction;
    }
    OptionalInt memberIndexOpt = _rsConfig.findMemberIndexByHostAndPort(target);
    if (!memberIndexOpt.isPresent()) {
      LOGGER.debug("replset: Could not find {} in current config so ignoring --"
          + " current config: {}", target, _rsConfig);
      HeartbeatResponseAction nextAction = HeartbeatResponseAction.makeNoAction();
      nextAction.setNextHeartbeatDelay(nextHeartbeatDelay);
      return nextAction;
    }
    assert memberIndexOpt.isPresent();
    int memberIndex = memberIndexOpt.getAsInt();

    MemberHeartbeatData hbData = _hbdata.get(memberIndex);
    assert hbData != null;
    MemberConfig member = _rsConfig.getMembers().get(memberIndex);
    if (!hbResponse.isOk()) {
      if (isUnauthorized) {
        LOGGER.debug("setAuthIssue: heartbeat response failed due to authentication"
            + " issue for member _id: {}", member.getId());
        hbData.setAuthIssue(now);
      } else if (hbStats.getNumFailuresSinceLastStart() > MAX_HEARTBEAT_RETRIES || alreadyElapsed
          .toMillis() >= _rsConfig.getHeartbeatTimeoutPeriod()) {
        LOGGER.debug("setDownValues: heartbeat response failed for member _id:{}"
            + ", msg: {}", member.getId(), hbResponse.getErrorDesc());

        hbData.setDownValues(now, hbResponse.getErrorDesc());
      } else {
        LOGGER.trace("Bad heartbeat response from {}; trying again; Retries left: {}; "
            + "{} ms have already elapsed",
            target,
            MAX_HEARTBEAT_RETRIES - hbStats.getNumFailuresSinceLastStart(),
            alreadyElapsed.toMillis()
        );
      }
    } else {
      ReplSetHeartbeatReply nonNullReply = commandReply.get();
      LOGGER.trace("setUpValues: heartbeat response good for member _id:{}, msg:  {}",
          member.getId(), nonNullReply.getHbmsg());
      hbData.setUpValues(now, member.getHostAndPort(), nonNullReply);
    }
    HeartbeatResponseAction nextAction = updateHeartbeatDataImpl(memberIndex, now);

    nextAction.setNextHeartbeatDelay(nextHeartbeatDelay);
    return nextAction;
  }

  /**
   * Performs updating {@link #_hbdata} and {@link #_currentPrimaryIndex} for
   * {@link #processHeartbeatResponse(org.threeten.bp.Instant, org.threeten.bp.Duration,
   * com.google.common.net.HostAndPort,
   * com.eightkdata.mongowp.client.core.MongoConnection.RemoteCommandResponse,
   * com.eightkdata.mongowp.OpTime) }.
   */
  private HeartbeatResponseAction updateHeartbeatDataImpl(int updatedConfigIndex, Instant now) {
    ////////////////////
    // Phase 1
    ////////////////////

    // If we believe the node whose data was just updated is primary, confirm that
    // the updated data supports that notion.  If not, erase our notion of who is primary.
    if (updatedConfigIndex == _currentPrimaryIndex) {
      final MemberHeartbeatData updatedHbData = _hbdata.get(updatedConfigIndex);
      assert updatedHbData != null;

      if (!updatedHbData.isUp() || updatedHbData.getState() != MemberState.RS_PRIMARY) {
        _currentPrimaryIndex = -1;
      }
    }

    HeartbeatResponseAction newAction;

    newAction = ifTwoPrimariesChecks(now);
    if (newAction != null) {
      return newAction;
    }

    // We do not believe that any remote is primary.
    assert _hbdata.stream().noneMatch(input ->
        input.isUp() && input.getState() == MemberState.RS_PRIMARY);
    assert _currentPrimaryIndex == -1;

    return HeartbeatResponseAction.makeNoAction();
  }

  /**
   * Scan the member list's heartbeat data for who is primary, update _currentPrimaryIndex if
   * necessary.
   *
   * @param now
   * @return the action that must be executed or null if no action have to be executed, in which
   *         case is guaranteed that there is no remote primary
   */
  @Nullable
  private HeartbeatResponseAction ifTwoPrimariesChecks(Instant now) {
    int remotePrimaryIndex = -1;
    for (int itIndex = 0; itIndex < _hbdata.size(); itIndex++) {
      MemberHeartbeatData it = _hbdata.get(itIndex);

      if (it.getState() == MemberState.RS_PRIMARY && it.isUp()) {
        if (remotePrimaryIndex != -1) {
          // two other nodes think they are primary (asynchronously polled)
          // -- wait for things to settle down.
          LOGGER.info("replSet info two remote primaries (transiently)");
          return HeartbeatResponseAction.makeNoAction();
        }
        remotePrimaryIndex = itIndex;
      }
    }

    if (remotePrimaryIndex != -1) {
      // If it's the same as last time, don't do anything further.
      if (_currentPrimaryIndex == remotePrimaryIndex) {
        return HeartbeatResponseAction.makeNoAction();
      }

      _currentPrimaryIndex = remotePrimaryIndex;
      return HeartbeatResponseAction.makeNoAction();
    } else {
      return null;
    }
  }

  private int getTotalPings() {
    int totalPings = 0;
    for (Entry<HostAndPort, PingStats> entry : _pings.entrySet()) {
      totalPings += entry.getValue().getCount();
    }
    return totalPings;
  }

  private boolean isBlacklistedMember(MemberConfig memberConfig, Instant now) {
    Instant blacklistedUntil = _syncSourceBlacklist.get(memberConfig.getHostAndPort());

    return blacklistedUntil != null && blacklistedUntil.isAfter(now);
  }

  /**
   * The MemberConfig of the primary node or null if there is no current primary.
   *
   * @return
   */
  @Nullable
  private MemberConfig getCurrentPrimaryMember() {
    if (_currentPrimaryIndex == -1) {
      return null;
    }

    return _rsConfig.getMembers().get(_currentPrimaryIndex);
  }

  private long getPing(HostAndPort hostAndPort) {
    return getPingOrDefault(hostAndPort).getAvgRoundTripAproximation();
  }

  private static class PingStats {

    @Nonnegative
    private long count = 0;
    @Nonnegative
    private long value = UnsignedInteger.MAX_VALUE.longValue();
    private Instant _lastHeartbeatStartDate = null;
    private int _numFailuresSinceLastStart = Integer.MAX_VALUE;

    /**
     * @return the number of {@link #hit(org.threeten.bp.Duration) 'hit'} calls.
     */
    @Nonnegative
    public long getCount() {
      return count;
    }

    /**
     * Returns the weighted average round trip time (in millis) for heartbeat messages to the
     * target.
     *
     * If no information is yet stored, {@link Long#MAX_VALUE} is returned
     *
     * @return the weighted average round trip time for heartbeat messages to the target.
     */
    @Nonnegative
    public long getAvgRoundTripAproximation() {
      return value;
    }

    public Instant getLastHeartbeatStartDate() {
      return _lastHeartbeatStartDate;
    }

    /**
     * Gets the number of failures since {@link #start(org.threeten.bp.Instant) 'start'} was last
     * called.
     * <p>
     * This value is incremented by calls to {@link #miss()}, cleared by calls to {@link #start()}
     * and set to {@link Integer#MAX_VALUE} by calls to
     * {@link #hit(org.threeten.bp.Duration) hit()}.
     */
    public int getNumFailuresSinceLastStart() {
      return _numFailuresSinceLastStart;
    }

    void start(Instant now) {
      _lastHeartbeatStartDate = now;
      _numFailuresSinceLastStart = 0;
    }

    private void miss() {
      ++_numFailuresSinceLastStart;
    }

    private void hit(Duration networkRoundTripTime) {
      _numFailuresSinceLastStart = Integer.MAX_VALUE;
      ++count;
      if (value == UnsignedInteger.MAX_VALUE.longValue()) { //first hit
        value = networkRoundTripTime.toMillis();
      } else {
        value = calculateAvgRoundTripAprox(networkRoundTripTime);
      }

      if (value > UnsignedInteger.MAX_VALUE.longValue()) {
        value = UnsignedInteger.MAX_VALUE.longValue();
      }
    }

    private long calculateAvgRoundTripAprox(Duration networkRoundTripTime) {
      return (long) ((value * 0.8) + (networkRoundTripTime.toMillis() * 0.2));
    }
  }
}
