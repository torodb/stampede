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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.WriteConcern.SyncMode;
import com.eightkdata.mongowp.WriteConcern.WType;
import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonType;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.FailedToParseException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.ArrayField;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ReplicaSetConfig {

  private static final IntField VERSION_FIELD = new IntField("version");
  private static final StringField ID_FIELD = new StringField("_id");
  private static final ArrayField MEMBERS_FIELD = new ArrayField("members");
  private static final DocField SETTINGS_FIELD = new DocField("settings");
  private static final String STEP_DOWN_CHECK_WRITE_CONCERN_MODE_FIELD_NAME = "$stepDownCheck";
  private static final LongField PROTOCOL_VERSION_FIELD = new LongField("protocolVersion");

  private static final IntField HEARTHBEAT_TIMEOUT_FIELD = new IntField("heartbeatTimeoutSecs");
  private static final BooleanField CHAINING_ALLOWED_FIELD = new BooleanField("chainingAllowed");
  @SuppressWarnings("checkstyle:LineLength")
  private static final DocField GET_LAST_ERROR_DEFAULTS_FIELD = new DocField("getLastErrorDefaults");
  private static final DocField GET_LAST_ERROR_MODES_FIELD = new DocField("getLastErrorModes");

  private static final int DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 10;
  private static final int DEFAULT_HEARTBEAT_TIMEOUT_MILLIS =
      DEFAULT_HEARTBEAT_TIMEOUT_SECONDS * 1000;
  private static final boolean DEFAULT_CHAINING_ALLOWED = true;

  private static final ImmutableSet<String> VALID_FIELD_NAMES = ImmutableSet.of(
      VERSION_FIELD.getFieldName(), ID_FIELD.getFieldName(),
      MEMBERS_FIELD.getFieldName(), SETTINGS_FIELD.getFieldName(),
      STEP_DOWN_CHECK_WRITE_CONCERN_MODE_FIELD_NAME,
      PROTOCOL_VERSION_FIELD.getFieldName()
  );

  private final String setName;
  private final int version;
  private final ImmutableList<MemberConfig> members;
  private final WriteConcern defaultWriteConcern;
  private final int heartbeatTimeoutPeriod;
  private final boolean chainingAllowed;
  private final int majorityVoteCount;
  private final int writeMajority;
  private final int totalVotingMembers;
  private final Map<String, ReplicaSetTagPattern> customWriteConcern;
  private final long protocolVersion;

  public ReplicaSetConfig(
      String id,
      int version,
      ImmutableList<MemberConfig> members,
      WriteConcern defaultWriteConcern,
      int heartbeatTimeoutPeriod,
      boolean chainingAllowed,
      Map<String, ReplicaSetTagPattern> customWriteConcern,
      long protocolVersion) {
    this.setName = id;
    this.version = version;
    this.defaultWriteConcern = defaultWriteConcern;
    this.heartbeatTimeoutPeriod = heartbeatTimeoutPeriod;
    this.chainingAllowed = chainingAllowed;
    this.protocolVersion = protocolVersion;
    this.customWriteConcern = customWriteConcern;

    int voters = 0;
    int arbiters = 0;
    for (MemberConfig member : members) {
      if (member.isArbiter()) {
        arbiters++;
      }
      if (member.isVoter()) {
        voters++;
      }
    }

    this.members = members;
    this.totalVotingMembers = voters;
    this.majorityVoteCount = voters / 2 + 1;
    this.writeMajority = Math.min(majorityVoteCount, voters - arbiters);
  }

  public String getReplSetName() {
    return setName;
  }

  public int getConfigVersion() {
    return version;
  }

  public ImmutableList<MemberConfig> getMembers() {
    return members;
  }

  public WriteConcern getDefaultWriteConcern() {
    return defaultWriteConcern;
  }

  /**
   * The heartbeat timeout period on millis.
   *
   * It cannot be negative if this config {@linkplain #validate(int, int) is valid}
   *
   * @return
   */
  public int getHeartbeatTimeoutPeriod() {
    return heartbeatTimeoutPeriod;
  }

  public boolean isChainingAllowed() {
    return chainingAllowed;
  }

  public int getMajorityVoteCount() {
    return majorityVoteCount;
  }

  public int getWriteMajority() {
    return writeMajority;
  }

  public int getTotalVotingMembers() {
    return totalVotingMembers;
  }

  public long getProtocolVersion() {
    return protocolVersion;
  }

  public Map<String, ReplicaSetTagPattern> getCustomWriteConcerns() {
    return Collections.unmodifiableMap(customWriteConcern);
  }

  /**
   * @param maxMembers       the maximun numbers of members a replication set should have. It could
   *                         be different on different MongoDB versions.
   * @param maxVotingMembers the maximun numbers of members who can vote in a replication set. It
   *                         could be different on different MongoDB versions.
   * @param customWCs        a mp of custom WriteConcerns
   * @throws BadValueException
   */
  public boolean validate(int maxMembers, int maxVotingMembers) throws BadValueException {
    if (version <= 0) {
      throw new BadValueException(VERSION_FIELD.getFieldName() + " field value of " + version
          + " is out of range");
    }
    if (setName.isEmpty()) {
      throw new BadValueException("Replica set configuration must have non-empty "
          + ID_FIELD.getFieldName() + " field");
    }
    if (heartbeatTimeoutPeriod < 0) {
      throw new BadValueException(SETTINGS_FIELD.getFieldName() + "."
          + HEARTHBEAT_TIMEOUT_FIELD.getFieldName() + " field value must be non-negative, "
          + "but found " + heartbeatTimeoutPeriod);
    }
    if (members.size() > maxMembers || members.isEmpty()) {
      throw new BadValueException("Replica set configuration contains " + members.size()
          + " members, but must have at least 1 and no more than " + maxMembers);
    }

    int localhostCount = 0;
    int voterCount = 0;
    int arbiterCount = 0;
    int electableCount = 0;

    for (int i = 0; i < members.size(); ++i) {
      MemberConfig memberI = members.get(i);
      memberI.validate();

      if (isLocalhost(memberI.getHostAndPort())) {
        ++localhostCount;
      }
      if (memberI.isVoter()) {
        ++voterCount;
      }
      // Nodes may be arbiters or electable, or neither, but never both.
      if (memberI.isArbiter()) {
        ++arbiterCount;
      } else if (memberI.getPriority() > 0) {
        ++electableCount;
      }
      for (int j = 0; j < members.size(); ++j) {
        if (i == j) {
          continue;
        }
        MemberConfig memberJ = members.get(j);
        if (memberI.getId() == memberJ.getId()) {
          throw new BadValueException("Found two member configurations with same "
              + ID_FIELD.getFieldName() + " field, " + MEMBERS_FIELD.getFieldName()
              + "." + i + "." + MEMBERS_FIELD.getFieldName()
              + " == " + MEMBERS_FIELD.getFieldName() + "." + j + "."
              + ID_FIELD.getFieldName() + " == " + memberI.getId());
        }
        if (memberI.getHostAndPort().equals(memberJ.getHostAndPort())) {
          throw new BadValueException("Found two member configurations with same "
              + MemberConfig.HOST_FIELD + " field, " + MEMBERS_FIELD.getFieldName()
              + "."
              + i + "." + MemberConfig.HOST_FIELD + " == "
              + MEMBERS_FIELD.getFieldName()
              + "." + j + "." + MemberConfig.HOST_FIELD + " == "
              + memberI.getHostAndPort().toString());
        }
      }
    }

    if (localhostCount != 0 && localhostCount != members.size()) {
      throw new BadValueException("Either all host names in a replica set configuration must "
          + "be localhost references, or none must be; found " + localhostCount + " out "
          + "of " + members.size());
    }

    if (voterCount > maxVotingMembers || voterCount == 0) {
      throw new BadValueException("Replica set configuration contains " + voterCount
          + " voting members, but must be at least 1 and no more than " + maxVotingMembers);
    }

    if (electableCount == 0) {
      throw new BadValueException("Replica set configuration must contain at least one "
          + "non-arbiter member with priority > 0");
    }

    switch (defaultWriteConcern.getWType()) {
      case INT:
        if (defaultWriteConcern.getWInt() == 0) {
          throw new BadValueException("Default write concern mode must wait for at least "
              + "1 member");
        }
        break;
      case TEXT:
        if (defaultWriteConcern.getWString().equals("majority")) {
          break;
        }
        if (!customWriteConcern.containsKey(defaultWriteConcern.getWString())) {
          throw new BadValueException("Default write concern requires undefined write "
              + "mode " + defaultWriteConcern.getWString());
        }
        break;
      default:
        throw new AssertionError("Unexpected write concern type " + defaultWriteConcern.getWType());
    }
    return true;
  }

  private static final boolean isLocalhost(HostAndPort hostAndPort) {
    String host = hostAndPort.getHostText();
    return host.equals("localhost")
        || host.startsWith("127.")
        || host.equals("::1")
        || host.equals("anonymous unix socket")
        || host.charAt(0) == '/';  // unix socket
  }

  public static ReplicaSetConfig fromDocument(@Nonnull BsonDocument bson)
      throws BadValueException, TypesMismatchException, NoSuchKeyException, FailedToParseException {
    BsonReaderTool.checkOnlyHasFields("replica set configuration", bson, VALID_FIELD_NAMES);

    String id = BsonReaderTool.getString(bson, ID_FIELD);

    int version = BsonReaderTool.getInteger(bson, VERSION_FIELD);

    Builder builder = new Builder(id, version);
    BsonArray uncastedMembers = BsonReaderTool.getArray(bson, MEMBERS_FIELD);
    int i = 0;
    for (BsonValue uncastedMember : uncastedMembers) {
      if (uncastedMember == null || !uncastedMember.isDocument()) {
        throw new TypesMismatchException(
            Integer.toString(i),
            "object",
            uncastedMember == null ? null : uncastedMember.getType()
        );
      }
      builder.addMemberConfig(MemberConfig.fromDocument(uncastedMember.asDocument()));
      i++;
    }

    BsonDocument settings;
    try {
      settings = BsonReaderTool.getDocument(bson, SETTINGS_FIELD);
    } catch (NoSuchKeyException ex) {
      settings = DefaultBsonValues.EMPTY_DOC;
    }

    builder
        .setHbTimeout(BsonReaderTool.getInteger(settings, HEARTHBEAT_TIMEOUT_FIELD,
            DEFAULT_HEARTBEAT_TIMEOUT_MILLIS))
        .setChainingAllowed(BsonReaderTool.getBoolean(settings, CHAINING_ALLOWED_FIELD,
            DEFAULT_CHAINING_ALLOWED));

    BsonDocument uncastedGetLastErrorDefaults = BsonReaderTool.getDocument(
        settings,
        GET_LAST_ERROR_DEFAULTS_FIELD
    );

    WriteConcern wc = WriteConcern.fromDocument(uncastedGetLastErrorDefaults);
    builder.setWriteConcern(wc);

    BsonDocument uncastedCustomWriteConcerns;
    try {
      uncastedCustomWriteConcerns = BsonReaderTool.getDocument(
          settings, GET_LAST_ERROR_MODES_FIELD);
    } catch (NoSuchKeyException ex) {
      uncastedCustomWriteConcerns = DefaultBsonValues.EMPTY_DOC;
    }
    Map<String, ReplicaSetTagPattern> customWriteConcernsBuilder = parseCustomWriteConcerns(
        uncastedCustomWriteConcerns);
    for (Map.Entry<String, ReplicaSetTagPattern> customWriteConcern : customWriteConcernsBuilder
        .entrySet()) {
      builder.putCustomWriteConcern(customWriteConcern.getKey(), customWriteConcern.getValue());
    }

    builder.setProtocolVersion(BsonReaderTool.getLong(bson, PROTOCOL_VERSION_FIELD));

    return builder.build();
  }

  public static class Builder {

    private final String setName;
    private final int version;
    private final ImmutableList.Builder<MemberConfig> membersBuilder = ImmutableList.builder();
    private int hbTimeout = DEFAULT_HEARTBEAT_TIMEOUT_MILLIS;
    boolean chainingAllowed = DEFAULT_CHAINING_ALLOWED;
    private WriteConcern wc = WriteConcern.with(SyncMode.NONE, 0, 0);
    private final Map<String, ReplicaSetTagPattern> customWriteConcernsBuilder = new HashMap<>();
    private long protocolVersion = 0;

    public Builder(String setName, int version) {
      this.setName = setName;
      this.version = version;
    }

    public Builder(ReplicaSetConfig other) {
      this.setName = other.setName;
      this.version = other.version;
      this.membersBuilder.addAll(other.members);
      this.hbTimeout = other.heartbeatTimeoutPeriod;
      this.chainingAllowed = other.chainingAllowed;
      this.wc = other.defaultWriteConcern;
      this.customWriteConcernsBuilder.putAll(other.customWriteConcern);
      this.protocolVersion = other.protocolVersion;
    }

    public Builder addMemberConfig(MemberConfig memberConfig) {
      this.membersBuilder.add(memberConfig);
      return this;
    }

    public Builder addAllMemberConfig(List<MemberConfig> memberConfigList) {
      this.membersBuilder.addAll(memberConfigList);
      return this;
    }

    public Builder setHbTimeout(int hbTimeout) {
      this.hbTimeout = hbTimeout;
      return this;
    }

    public Builder setChainingAllowed(boolean chainingAllowed) {
      this.chainingAllowed = chainingAllowed;
      return this;
    }

    public Builder setWriteConcern(WriteConcern wc) {
      this.wc = wc;
      return this;
    }

    public Builder setProtocolVersion(long protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
    }

    public Builder putCustomWriteConcern(String key, ReplicaSetTagPattern value) {
      this.customWriteConcernsBuilder.put(key, value);
      return this;
    }

    public ReplicaSetConfig build() {
      return new ReplicaSetConfig(
          setName,
          version,
          membersBuilder.build(),
          wc,
          hbTimeout,
          chainingAllowed,
          customWriteConcernsBuilder,
          protocolVersion
      );
    }
  }

  public Status<ReplicaSetTagPattern> getCustomWriteConcernTagPattern(String patternName) {
    ReplicaSetTagPattern result = customWriteConcern.get(patternName);
    if (result == null) {
      return Status.from(ErrorCode.UNKNOWN_REPL_WRITE_CONCERN, "No write concern mode "
          + "named '" + patternName + "' found in replica set configuration");
    }
    return Status.ok(result);
  }

  private static Map<String, ReplicaSetTagPattern> parseCustomWriteConcerns(BsonDocument bson)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    Map<String, ReplicaSetTagPattern> map = new HashMap<>(bson.size());
    for (Entry<?> customWriteNameEntry : bson) {
      BsonDocument constraintDoc = BsonReaderTool.getDocument(bson, customWriteNameEntry.getKey());
      Map<String, Integer> constraintMap = new HashMap<>(constraintDoc.size());

      for (Entry<?> tagEntry : constraintDoc) {
        int intValue;
        try {
          intValue = tagEntry.getValue().asNumber().intValue();
        } catch (UnsupportedOperationException ex) {
          String fieldName =
              SETTINGS_FIELD.getFieldName()
              + '.' + GET_LAST_ERROR_MODES_FIELD.getFieldName()
              + '.' + customWriteNameEntry
              + '.' + constraintDoc;
          BsonType tagType = tagEntry.getValue().getType();
          throw new TypesMismatchException(
              fieldName,
              "number",
              tagType,
              "Expected " + fieldName + " to be a number, not " + tagType.toString().toLowerCase(
                  Locale.ROOT)
          );
        }
        if (intValue <= 0) {
          String fieldName =
              SETTINGS_FIELD.getFieldName()
              + '.' + GET_LAST_ERROR_MODES_FIELD.getFieldName()
              + '.' + customWriteNameEntry
              + '.' + constraintDoc;
          throw new BadValueException("Value of " + fieldName + " must be positive, but found "
              + intValue);
        }
        constraintMap.put(tagEntry.getKey(), intValue);
      }
      map.put(customWriteNameEntry.getKey(), new ReplicaSetTagPattern(constraintMap));
    }
    return map;
  }

  public BsonDocument toBson() {
    BsonDocumentBuilder result = new BsonDocumentBuilder();
    result.append(ID_FIELD, setName);
    result.append(VERSION_FIELD, version);

    BsonArrayBuilder membersList = new BsonArrayBuilder();
    for (MemberConfig member : members) {
      membersList.add(member.toBson());
    }
    result.append(MEMBERS_FIELD, membersList.build());

    BsonDocumentBuilder settingsBuilder = new BsonDocumentBuilder();
    settingsBuilder.append(CHAINING_ALLOWED_FIELD, chainingAllowed);
    settingsBuilder.append(HEARTHBEAT_TIMEOUT_FIELD, heartbeatTimeoutPeriod);

    BsonDocumentBuilder customWrites = new BsonDocumentBuilder();
    for (Map.Entry<String, ReplicaSetTagPattern> entry : customWriteConcern.entrySet()) {
      String customWriteName = entry.getKey();
      if (customWriteName.startsWith("$")) { //MongoDB uses $ as an internal mode
        continue;
      }
      BsonDocument tagMap = entry.getValue().toBson();
      customWrites.appendUnsafe(customWriteName, tagMap);
    }
    settingsBuilder.append(GET_LAST_ERROR_MODES_FIELD, customWrites);
    settingsBuilder.append(GET_LAST_ERROR_DEFAULTS_FIELD, defaultWriteConcern.toDocument());
    settingsBuilder.append(PROTOCOL_VERSION_FIELD, protocolVersion);

    result.append(SETTINGS_FIELD, settingsBuilder);
    return result.build();
  }

  @Override
  public String toString() {
    return toBson().toString();
  }

  public Status<?> checkIfWriteConcernCanBeSatisfied(WriteConcern writeConcern) {
    if (writeConcern.getWType() == WType.TEXT && !writeConcern.getWString().equals("majority")) {
      Status<ReplicaSetTagPattern> pattern = getCustomWriteConcernTagPattern(writeConcern
          .getWString());
      if (!pattern.isOk()) {
        return pattern;
      }

      ReplicaSetTagMatch matcher = pattern.getResult().matcher();
      for (MemberConfig member : getMembers()) {
        for (Map.Entry<String, String> entry : member.getTags().entrySet()) {
          if (matcher.update(entry.getKey(), entry.getValue())) {
            return Status.ok();
          }
        }
      }
      // Even if all the nodes in the set had a given write it still would not satisfy this
      // write concern mode.
      return Status.from(ErrorCode.CANNOT_SATISFY_WRITE_CONCERN, "Not enough nodes match "
          + "write concern mode \"" + writeConcern.getWString() + "\"");
    } else {
      int nodesRemaining = writeConcern.getWInt();
      for (MemberConfig member : getMembers()) {
        if (!member.isArbiter()) {
          nodesRemaining--;
        }
      }
      if (nodesRemaining <= 0) {
        return Status.ok();
      }
      return Status.from(ErrorCode.CANNOT_SATISFY_WRITE_CONCERN, "Not enough data-bearing nodes");
    }
  }

  @Nullable
  public MemberConfig findMemberById(int memberId) {
    for (MemberConfig member : members) {
      if (member.getId() == memberId) {
        return member;
      }
    }
    return null;
  }

  /**
   *
   * @param currentSource
   * @return the index of the member with the given host and port or empty if there is no member
   *         with that host and port
   */
  public OptionalInt findMemberIndexByHostAndPort(HostAndPort currentSource) {
    for (int i = 0; i < members.size(); i++) {
      MemberConfig member = members.get(i);
      if (member.getHostAndPort().equals(currentSource)) {
        return OptionalInt.of(i);
      }
    }
    return OptionalInt.empty();
  }

  /**
   * Representation of a tag matching pattern, like { "dc": 2, "rack": 3 }, of the form used for
   * tagged replica set writes.
   *
   * Patterns match on a set of members iff, for each tag there are, at least <em>i</em> different
   * members where the tag is defined and the value of the tag key is different between them, where
   * <em>i</em> is the value of the tag on the pattern constraint.
   */
  public static class ReplicaSetTagPattern {

    private final Map<String, Integer> constraints;

    public ReplicaSetTagPattern(Map<String, Integer> constraints) {
      this.constraints = constraints;
    }

    public ReplicaSetTagMatch matcher() {
      return new ReplicaSetTagMatch(this);
    }

    private BsonDocument toBson() {
      BsonDocumentBuilder result = new BsonDocumentBuilder();
      for (java.util.Map.Entry<String, Integer> entry : constraints.entrySet()) {
        result.appendUnsafe(entry.getKey(), DefaultBsonValues.newInt(entry.getValue()));
      }
      return result.build();
    }
  }

  /**
   * State object for progressive detection of {@link ReplicaSetTagPattern} constraint satisfaction.
   * <p>
   * This is an abstraction of the replica set write tag satisfaction problem.
   * <p>
   * Replica set tag matching is an event-driven constraint satisfaction process. This type
   * represents the state of that process. It is initialized from a pattern object, then
   * progressively updated with tags. After processing a sequence of tags sufficient to satisfy the
   * pattern, isSatisfied() becomes true.
   */
  public static class ReplicaSetTagMatch {

    private final Map<String, BoundTagValue> boundTagValues;

    private ReplicaSetTagMatch(ReplicaSetTagPattern pattern) {
      boundTagValues = new HashMap<>(pattern.constraints.size());
      for (java.util.Map.Entry<String, Integer> entry : pattern.constraints.entrySet()) {
        boundTagValues.put(entry.getKey(), new BoundTagValue(entry.getValue()));
      }
    }

    public boolean update(String key, String value) {
      BoundTagValue bound = boundTagValues.get(key);
      if (bound != null) {
        bound.boundTagValues.add(value);
      }
      return isSatisfied();
    }

    public boolean isSatisfied() {
      for (BoundTagValue value : boundTagValues.values()) {
        if (value.boundTagValues.size() < value.min) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Representation of the state related to a single tag key in the match pattern.
   * <p>
   * Consists of a constraint (key index and min count for satisfaction) and a list of already
   * observed values.
   * <p>
   * A BoundTagValue is satisfied when the size of boundValues is at least constraint.getMinCount().
   */
  private static class BoundTagValue {

    private final int min;
    private final Set<String> boundTagValues;

    public BoundTagValue(int min) {
      this.min = min;
      this.boundTagValues = new HashSet<>(min);
    }
  }
}
