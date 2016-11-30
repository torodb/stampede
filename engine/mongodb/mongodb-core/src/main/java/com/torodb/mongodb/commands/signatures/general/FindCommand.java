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

package com.torodb.mongodb.commands.signatures.general;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonType;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.FailedToParseException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.mongodb.commands.pojos.CursorResult;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindArgument;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindResult;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.LongConsumer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public class FindCommand extends AbstractNotAliasableCommand<FindArgument, FindResult> {

  public static final FindCommand INSTANCE = new FindCommand();
  private static final String COMMAND_NAME = "find";
  private static final Function<BsonDocument, BsonDocument> IDENTITY = (o) -> o;
  private static final Function<BsonValue<?>, BsonDocument> DOWN_CAST = (o) -> o.asDocument();

  private FindCommand() {
    super(COMMAND_NAME);
  }

  @Override
  public boolean isSlaveOverrideOk() {
    return true;
  }

  @Override
  public boolean isSlaveOk() {
    return false;
  }

  @Override
  public boolean supportsReadConcern() {
    return true;
  }

  @Override
  public boolean isAllowedOnMaintenance() {
    return false;
  }

  @Override
  public boolean shouldAffectCommandCounter() {
    return false;
  }

  @Override
  public Class<? extends FindArgument> getArgClass() {
    return FindArgument.class;
  }

  @Override
  public FindArgument unmarshallArg(BsonDocument requestDoc) throws BadValueException,
      TypesMismatchException, NoSuchKeyException, FailedToParseException {
    return FindArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(FindArgument request) throws MarshalException {
    return request.marshall();
  }

  @Override
  public Class<? extends FindResult> getResultClass() {
    return FindResult.class;
  }

  @Override
  public FindResult unmarshallResult(BsonDocument resultDoc) throws BadValueException,
      TypesMismatchException, NoSuchKeyException, FailedToParseException, MongoException {
    return FindResult.unmarshall(resultDoc);
  }

  @Override
  public BsonDocument marshallResult(FindResult result) throws MarshalException {
    return result.marshall();
  }

  public static class FindArgument {

    private static final String FILTER_FIELD_NAME = "filter";
    private static final String PROJECTION_FIELD_NAME = "projection";
    private static final String SORT_FIELD_NAME = "sort";
    private static final String HINT_FIELD_NAME = "hint";
    private static final String SKIP_FIELD_NAME = "skip";
    private static final String LIMIT_FIELD_NAME = "limit";
    private static final String BATCH_FIELD_NAME = "batchSize";
    private static final String N_TO_RETURN_FIELD_NAME = "ntoreturn";
    private static final String SINGLE_BATCH_FIELD_NAME = "singleBatch";
    private static final String COMMENT_FIELD_NAME = "comment";
    private static final String MAX_SCAN_FIELD_NAME = "maxScan";
    private static final String MAX_FIELD_NAME = "max";
    private static final String MIN_FIELD_NAME = "min";
    private static final String RETURN_KEY_FIELD_NAME = "returnKey";
    private static final String SHOW_RECORD_ID_FIELD_NAME = "showRecordId";
    private static final String SNAPSHOT_FIELD_NAME = "snapshot";
    private static final String TAILABLE_FIELD_NAME = "tailable";
    private static final String OPLOG_REPLY_FIELD_NAME = "oplogReplay";
    private static final String NO_CURSOR_TIMEOUT_FIELD_NAME = "noCursorTimeout";
    private static final String AWAIT_DATA_FIELD_NAME = "awaitData";
    private static final String PARTIAL_RESULTS_FIELD_NAME = "allowPartialResults";
    private static final String TERM_FIELD_NAME = "term";
    private static final String READ_CONCERN_FIELD_NAME = "readConcern";
    private static final String CMD_OPTION_MAX_TIME_MS_FIELD_NAME = "maxTimeMS";

    private String collection;
    private BsonDocument filter;
    private BsonDocument proj;
    private BsonDocument sort;
    /**
     * The hint provided, if any. If the hint was by index key pattern, the value of '_hint' is the
     * key pattern hinted. If the hint was by index name, the value of '_hint' is {$hint:
     * &lt;String&gt;}, where &lt;String&gt; is the index name hinted.
     */
    private BsonDocument hint;
    // The read concern is parsed elsewhere.
    private BsonDocument readConcern;

    private boolean wantMore = true;

    /**
     * Negative skip is illegal and a skip of zero received from the client is interpreted as the
     * absence of a skip value.
     */
    @Nonnegative
    private long skip;

    /**
     * Negative limit is illegal and a limit value of zero received from the client is interpreted
     * as the absence of a limit value.
     */
    @Nonnegative
    private long limit;

    /**
     * Must be either unset or non-negative. Negative batchSize is illegal but batchSize of 0 is
     * allowed.
     */
    @Nonnegative
    private long batchSize;

    /**
     * Set only when parsed from an OP_QUERY find message. The value is computed by driver or shell
     * and is set to be a min of batchSize and limit provided by user. LPQ can have set either
     * ntoreturn or batchSize / limit.
     */
    @Nonnegative
    private long ntoreturn;

    private boolean explain = false;

    private String comment;

    private int maxScan = 0;
    private int maxTimeMs = 0;

    private BsonDocument min;
    private BsonDocument max;

    private boolean returnKey = false;
    boolean showRecordId = false;
    boolean snapshot = false;
    boolean hasReadPref = false;

    // Options that can be specified in the OP_QUERY 'flags' header.
    boolean tailable = false;
    boolean slaveOk = false;
    boolean oplogReplay = false;
    boolean noCursorTimeout = false;
    boolean awaitData = false;
    boolean exhaust = false;
    boolean allowPartialResults = false;

    private long replicationTerm;

    public FindArgument(String collection, BsonDocument filter,
        BsonDocument proj, BsonDocument sort, BsonDocument hint, BsonDocument readConcern,
        long skip, long limit, long batchSize, long ntoreturn, String comment,
        BsonDocument min, BsonDocument max, long replicationTerm, int maxScan, int maxTimeMs) {
      this.collection = collection;
      this.filter = filter;
      this.proj = proj;
      this.sort = sort;
      this.hint = hint;
      this.readConcern = readConcern;
      this.skip = skip;
      this.limit = limit;
      this.batchSize = batchSize;
      this.ntoreturn = ntoreturn;
      this.comment = comment;
      this.min = min;
      this.max = max;
      this.replicationTerm = replicationTerm;
      this.maxScan = maxScan;
      this.maxTimeMs = maxTimeMs;
    }

    private static void checkFieldType(Entry<?> entry, BsonType expectedType) throws
        FailedToParseException {
      if (entry.getValue().getType() != expectedType) {
        throw new FailedToParseException("Failed to parse " + entry.getKey()
            + ". It sould be of BSON type " + expectedType);
      }
      return;
    }

    private static boolean isTextScoreMeta(BsonValue<?> value) {
      // value must be {$meta: "textScore"}
      if (value.isDocument()) {
        return false;
      }
      BsonDocument metaObj = value.asDocument();
      if (metaObj.size() != 1) { //must have exactly 1 element
        return false;
      }
      Entry<?> metaEntry = metaObj.getFirstEntry();
      if (metaEntry.getKey().equals("$meta")) {
        return false;
      }
      if (metaEntry.getValue().isString()) {
        return false;
      }
      return metaEntry.getValue().asString().getValue().equals("textScore");
    }

    private static boolean isValidSortOrder(BsonDocument sortObj) {
      for (Entry<?> entry : sortObj) {
        if (entry.getKey().isEmpty()) {
          return false;
        }
        if (isTextScoreMeta(entry.getValue())) {
          continue;
        }
        long n;
        if (entry.getValue().isNumber()) {
          n = entry.getValue().asNumber().longValue();
        } else {
          return false;
        }
        if (n != 1 && n != -1) {
          return false;
        }
      }
      return true;
    }

    private static void parseNonNegativeNumber(BsonDocument requestDoc, BsonValue<?> value,
        String fieldName, LongConsumer consumer) throws FailedToParseException, BadValueException {
      if (!value.isNumber()) {
        throw new FailedToParseException("Failed to parse: " + requestDoc + ". "
            + "'" + fieldName + "' field must be numeric");
      }
      long n = value.asNumber().longValue();
      if (n < 0) {
        throw new BadValueException(fieldName + " value must be non-negative");
      }
      if (n > 0) {
        consumer.accept(n);
      }
    }

    @SuppressFBWarnings("FE_FLOATING_POINT_EQUALITY")
    private static int parseMaxTimeMs(Entry<?> entry) throws BadValueException {
      if (!entry.getValue().isNumber()) {
        throw new BadValueException(entry.getKey() + " must be a number");
      }
      long maxTimeMsLong = entry.getValue().asNumber().longValue();
      if (maxTimeMsLong < 0 || maxTimeMsLong > Integer.MAX_VALUE) {
        throw new BadValueException(entry.getKey() + " is out of range");
      }
      double maxTimeMsDouble = entry.getValue().asNumber().doubleValue();
      if (entry.getValue().getType() == BsonType.DOUBLE && Math.floor(maxTimeMsDouble)
          == maxTimeMsDouble) {
        throw new BadValueException(entry.getKey() + " has non-integral value");
      }
      return (int) maxTimeMsLong;
    }

    private static FindArgument unmarshall(BsonDocument requestDoc) throws FailedToParseException,
        TypesMismatchException, NoSuchKeyException, BadValueException {
      Builder builder = new Builder();
      builder.setExplain(false);

      for (Entry<?> entry : requestDoc) {
        String fieldName = entry.getKey();
        BsonValue<?> value = entry.getValue();
        switch (fieldName) {
          case COMMAND_NAME: {
            checkFieldType(entry, BsonType.STRING);
            builder.setCollection(value.asString().getValue());
            continue;
          }
          case FILTER_FIELD_NAME: {
            checkFieldType(entry, BsonType.DOCUMENT);
            builder.setFilter(value.asDocument());
            continue;
          }
          case PROJECTION_FIELD_NAME: {
            checkFieldType(entry, BsonType.DOCUMENT);
            builder.setProj(value.asDocument());
            continue;
          }
          case SORT_FIELD_NAME: {
            checkFieldType(entry, BsonType.DOCUMENT);
            BsonDocument sortDoc = value.asDocument();
            if (!isValidSortOrder(sortDoc)) {
              throw new BadValueException("bad sort specification");
            }
            builder.setSort(sortDoc);
            continue;
          }
          case HINT_FIELD_NAME: {
            BsonDocument hintObj;
            if (entry.getValue().isDocument()) {
              hintObj = entry.getValue().asDocument();
            } else if (entry.getValue().isString()) {
              hintObj = DefaultBsonValues.newDocument("$hint", entry.getValue());
            } else {
              throw new FailedToParseException("hint must be either a string or a nested object");
            }
            builder.setHint(hintObj);
            continue;
          }
          case READ_CONCERN_FIELD_NAME: {
            checkFieldType(entry, BsonType.DOCUMENT);
            builder.setReadConcern(entry.getValue().asDocument());
            continue;
          }
          case SKIP_FIELD_NAME: {
            parseNonNegativeNumber(requestDoc, value, fieldName, n -> builder.setSkip(n));
            continue;
          }
          case LIMIT_FIELD_NAME: {
            parseNonNegativeNumber(requestDoc, value, fieldName, n -> builder.setLimit(n));
            continue;
          }
          case BATCH_FIELD_NAME: {
            parseNonNegativeNumber(requestDoc, value, fieldName, n -> builder.setBatchSize(n));
            continue;
          }
          case N_TO_RETURN_FIELD_NAME: {
            parseNonNegativeNumber(requestDoc, value, fieldName, n -> builder.setNtoreturn(n));
            continue;
          }
          case SINGLE_BATCH_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setWantMore(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case COMMENT_FIELD_NAME: {
            checkFieldType(entry, BsonType.STRING);
            builder.setComment(value.asString().getValue());
            continue;
          }
          case MAX_SCAN_FIELD_NAME: {
            parseNonNegativeNumber(requestDoc, value, fieldName, n -> builder.setMaxScan((int) n));
            continue;
          }
          case CMD_OPTION_MAX_TIME_MS_FIELD_NAME: {
            builder.setMaxTimeMs(parseMaxTimeMs(entry));
            continue;
          }
          case MIN_FIELD_NAME: {
            checkFieldType(entry, BsonType.DOCUMENT);
            builder.setMin(value.asDocument());
            continue;
          }
          case MAX_FIELD_NAME: {
            checkFieldType(entry, BsonType.DOCUMENT);
            builder.setMax(value.asDocument());
            continue;
          }
          case RETURN_KEY_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setReturnKey(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case SHOW_RECORD_ID_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setShowRecordId(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case SNAPSHOT_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setSnapshot(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case TAILABLE_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setTailable(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case OPLOG_REPLY_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setOplogReplay(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case NO_CURSOR_TIMEOUT_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setNoCursorTimeout(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case AWAIT_DATA_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setAwaitData(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case PARTIAL_RESULTS_FIELD_NAME: {
            checkFieldType(entry, BsonType.BOOLEAN);
            builder.setAllowPartialResults(value.asBoolean().getPrimitiveValue());
            continue;
          }
          case TERM_FIELD_NAME: {
            checkFieldType(entry, BsonType.INT64);
            builder.setReplicationTerm(value.asInt64().getValue());
            continue;
          }
          default:
        }
        if (fieldName.charAt(0) == '$') {
          throw new FailedToParseException("Failed to parse: " + COMMENT_FIELD_NAME + ". "
              + "Unrecognized field '" + fieldName + "'.");
        }
      }
      builder.addMetaProjection();
      builder.validateFindCmd();
      return builder.build();
    }

    private BsonDocument marshall() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getCollection() {
      return collection;
    }

    public BsonDocument getFilter() {
      return filter;
    }

    public BsonDocument getProjection() {
      return proj;
    }

    public BsonDocument getSort() {
      return sort;
    }

    public BsonDocument getHint() {
      return hint;
    }

    public BsonDocument getReadConcern() {
      return readConcern;
    }

    public long getSkip() {
      return skip;
    }

    public long getLimit() {
      return limit;
    }

    public long getBatchSize() {
      return batchSize;
    }

    public long getNtoreturn() {
      return ntoreturn;
    }

    /**
     * Returns batchSize or ntoreturn value if either is set. If neither is set, returns
     * {@link OptionalLong#EMPTY}
     */
    public OptionalLong getEffectiveBatchSize() {
      if (batchSize > 0) {
        return OptionalLong.of(batchSize);
      } else if (ntoreturn > 0) {
        return OptionalLong.of(ntoreturn);
      }
      return OptionalLong.empty();
    }

    public boolean isWantMore() {
      return wantMore;
    }

    public boolean isExplain() {
      return explain;
    }

    public String getComment() {
      return comment;
    }

    public int getMaxScan() {
      return maxScan;
    }

    public int getMaxTimeMs() {
      return maxTimeMs;
    }

    public BsonDocument getMin() {
      return min;
    }

    public BsonDocument getMax() {
      return max;
    }

    boolean returnKey() {
      return returnKey;
    }

    boolean showRecordId() {
      return showRecordId;
    }

    boolean isSnapshot() {
      return snapshot;
    }

    boolean hasReadPref() {
      return hasReadPref;
    }

    boolean isTailable() {
      return tailable;
    }

    boolean isSlaveOk() {
      return slaveOk;
    }

    boolean isOplogReplay() {
      return oplogReplay;
    }

    boolean isNoCursorTimeout() {
      return noCursorTimeout;
    }

    boolean isAwaitData() {
      return awaitData;
    }

    boolean isExhaust() {
      return exhaust;
    }

    boolean isAllowPartialResults() {
      return allowPartialResults;
    }

    @SuppressFBWarnings("URF_UNREAD_FIELD")
    public static class Builder {

      private String collection;
      @Nonnull
      private BsonDocument filter = DefaultBsonValues.EMPTY_DOC;
      @Nonnull
      private BsonDocument proj = DefaultBsonValues.EMPTY_DOC;
      @Nonnull
      private BsonDocument sort = DefaultBsonValues.EMPTY_DOC;
      /**
       * The hint provided, if any. If the hint was by index key pattern, the value of '_hint' is
       * the key pattern hinted. If the hint was by index name, the value of '_hint' is {$hint:
       * &lt;String&gt;}, where &lt;String&gt; is the index name hinted.
       */
      @Nonnull
      private BsonDocument hint = DefaultBsonValues.EMPTY_DOC;
      // The read concern is parsed elsewhere.
      @Nonnull
      private BsonDocument readConcern = DefaultBsonValues.EMPTY_DOC;

      private boolean wantMore = true;

      /**
       * Negative skip is illegal and a skip of zero received from the client is interpreted as the
       * absence of a skip value.
       */
      @Nonnegative
      private long skip;

      /**
       * Negative limit is illegal and a limit value of zero received from the client is interpreted
       * as the absence of a limit value.
       */
      @Nonnegative
      private long limit;

      /**
       * Must be either unset or non-negative. Negative batchSize is illegal but batchSize of 0 is
       * allowed.
       */
      @Nonnegative
      private long batchSize;

      /**
       * Set only when parsed from an OP_QUERY find message. The value is computed by driver or
       * shell and is set to be a min of batchSize and limit provided by user. LPQ can have set
       * either ntoreturn or batchSize / limit.
       */
      @Nonnegative
      private long ntoreturn;

      private boolean explain = false;

      private String comment;

      private int maxScan = 0;
      private int maxTimeMs = 0;

      @Nonnull
      private BsonDocument min = DefaultBsonValues.EMPTY_DOC;
      @Nonnull
      private BsonDocument max = DefaultBsonValues.EMPTY_DOC;

      private boolean returnKey = false;
      boolean showRecordId = false;
      boolean snapshot = false;
      boolean hasReadPref = false;

      // Options that can be specified in the OP_QUERY 'flags' header.
      boolean tailable = false;
      boolean slaveOk = false;
      boolean oplogReplay = false;
      boolean noCursorTimeout = false;
      boolean awaitData = false;
      boolean exhaust = false;
      boolean allowPartialResults = false;

      private long replicationTerm;

      public Builder setCollection(@Nonnull String collection) {
        this.collection = collection;
        return this;
      }

      public Builder setFilter(@Nonnull BsonDocument filter) {
        this.filter = filter;
        return this;
      }

      public Builder setProj(@Nonnull BsonDocument proj) {
        this.proj = proj;
        return this;
      }

      public Builder setSort(@Nonnull BsonDocument sort) {
        this.sort = sort;
        return this;
      }

      public Builder setHint(@Nonnull BsonDocument hint) {
        this.hint = hint;
        return this;
      }

      public Builder setReadConcern(@Nonnull BsonDocument readConcern) {
        this.readConcern = readConcern;
        return this;
      }

      public Builder setWantMore(boolean wantMore) {
        this.wantMore = wantMore;
        return this;
      }

      public Builder setSkip(long skip) {
        this.skip = skip;
        return this;
      }

      public Builder setLimit(long limit) {
        this.limit = limit;
        return this;
      }

      public Builder setBatchSize(long batchSize) {
        this.batchSize = batchSize;
        return this;
      }

      public Builder setNtoreturn(long ntoreturn) {
        this.ntoreturn = ntoreturn;
        return this;
      }

      public Builder setExplain(boolean explain) {
        this.explain = explain;
        return this;
      }

      public Builder setComment(String comment) {
        this.comment = comment;
        return this;
      }

      public Builder setMaxScan(int maxScan) {
        this.maxScan = maxScan;
        return this;
      }

      public Builder setMaxTimeMs(int maxTimeMs) {
        this.maxTimeMs = maxTimeMs;
        return this;
      }

      public Builder setMin(BsonDocument min) {
        Preconditions.checkArgument(min != null, "Min cannot be null");
        this.min = min;
        return this;
      }

      public Builder setMax(BsonDocument max) {
        Preconditions.checkArgument(max != null, "Max cannot be null");
        this.max = max;
        return this;
      }

      public Builder setReturnKey(boolean returnKey) {
        this.returnKey = returnKey;
        return this;
      }

      public Builder setShowRecordId(boolean showRecordId) {
        this.showRecordId = showRecordId;
        return this;
      }

      public Builder setSnapshot(boolean snapshot) {
        this.snapshot = snapshot;
        return this;
      }

      public Builder setHasReadPref(boolean hasReadPref) {
        this.hasReadPref = hasReadPref;
        return this;
      }

      public Builder setTailable(boolean tailable) {
        this.tailable = tailable;
        return this;
      }

      public Builder setSlaveOk(boolean slaveOk) {
        this.slaveOk = slaveOk;
        return this;
      }

      public Builder setOplogReplay(boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
      }

      public Builder setNoCursorTimeout(boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
      }

      public Builder setAwaitData(boolean awaitData) {
        this.awaitData = awaitData;
        return this;
      }

      public Builder setExhaust(boolean exhaust) {
        this.exhaust = exhaust;
        return this;
      }

      public Builder setAllowPartialResults(boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
        return this;
      }

      public Builder setReplicationTerm(long replicationTerm) {
        this.replicationTerm = replicationTerm;
        return this;
      }

      public FindArgument build() {
        return new FindArgument(collection, filter, proj, sort, hint, readConcern,
            skip, limit, batchSize, ntoreturn, comment, min, max, replicationTerm,
            maxScan, maxTimeMs);
      }

      public void addMetaProjection() {
        if (returnKey) {
          BsonDocumentBuilder projBob;
          projBob = new BsonDocumentBuilder(proj);
          // We use $$ because it's never going to show up in a user's projection.
          // The exact text doesn't matter.
          projBob.append(
              new DocField("$$"),
              DefaultBsonValues.newDocument(
                  "$meta",
                  DefaultBsonValues.newString("indexKey"))
          );
          proj = projBob.build();
        }
        if (showRecordId) {
          BsonDocumentBuilder projBob;
          projBob = new BsonDocumentBuilder(proj);

          projBob.append(
              new DocField("$recordId"),
              DefaultBsonValues.newDocument(
                  "$meta",
                  DefaultBsonValues.newString("recordId"))
          );
          proj = projBob.build();
        }
      }

      public void validateFindCmd() throws BadValueException {
        if (awaitData && !tailable) {
          throw new BadValueException("Cannot set awaitData without tailable");
        }
        validate();
      }

      public void validate() throws BadValueException {
        // Min and Max objects must have the same fields.
        if (!min.isEmpty() && !max.isEmpty()) {
          UnmodifiableIterator<Entry<?>> minIt = min.iterator();
          UnmodifiableIterator<Entry<?>> maxIt = max.iterator();
          while (minIt.hasNext() && maxIt.hasNext()) {
            if (!minIt.next().getKey().equals(maxIt.next().getKey())) {
              throw new BadValueException("min and max must have the same field names");
            }
          }
          if (minIt.hasNext() || maxIt.hasNext()) {
            throw new BadValueException("min and max must have the same field names");
          }
        }

        // Can't combine a normal sort and a $meta projection on the same field.
        for (Entry<?> entry : proj) {
          if (isTextScoreMeta(entry.getValue())) {
            BsonValue<?> valueOnSort = sort.get(entry.getKey());
            if (valueOnSort != null && !isTextScoreMeta(valueOnSort)) {
              throw new BadValueException("can't have a non-$meta sort on a $meta projection");
            }
          }
        }

        // All fields with a $meta sort must have a corresponding $meta projection.
        for (Entry<?> entry : sort) {
          if (isTextScoreMeta(entry.getValue())) {
            BsonValue<?> valueOnProj = proj.get(entry.getKey());
            if (valueOnProj == null || !isTextScoreMeta(valueOnProj)) {
              throw new BadValueException("must have $meta projection for all $meta sort keys");
            }
          }
        }

        if (snapshot) {
          if (!sort.isEmpty()) {
            throw new BadValueException("E12001 can't use sort with snapshot");
          }
          if (!hint.isEmpty()) {
            throw new BadValueException("E12002 can't use hint with snapshot");
          }
        }

        if ((limit > 0 || batchSize > 0) && ntoreturn > 0) {
          throw new BadValueException(
              "'limit' or 'batchSize' fields can not be set with 'ntoreturn' field.");
        }

        // Tailable cursors cannot have any sort other than {$natural: 1}.
        if (tailable) {
          if (!sort.isEmpty() && !sort.equals(DefaultBsonValues.newDocument("$natural",
              DefaultBsonValues.INT32_ONE))) {
            throw new BadValueException(
                "cannot use tailable option with a sort other than {$natural: 1}");
          }
        }
      }
    }
  }

  public static class FindResult {

    private final CursorResult<BsonDocument> cursor;

    public FindResult(CursorResult<BsonDocument> cursor) {
      this.cursor = cursor;
    }

    public CursorResult<BsonDocument> getCursor() {
      return cursor;
    }

    private static FindResult unmarshall(BsonDocument resultDoc) throws BadValueException,
        TypesMismatchException, NoSuchKeyException {

      return new FindResult(CursorResult.unmarshall(resultDoc, DOWN_CAST));
    }

    private BsonDocument marshall() {
      return cursor.marshall(IDENTITY);
    }

  }

}
