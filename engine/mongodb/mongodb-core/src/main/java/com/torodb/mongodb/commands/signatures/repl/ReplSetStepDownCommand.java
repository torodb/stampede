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

package com.torodb.mongodb.commands.signatures.repl;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonInt32;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.torodb.mongodb.commands.signatures.repl.ReplSetStepDownCommand.ReplSetStepDownArgument;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
public class ReplSetStepDownCommand
    extends AbstractNotAliasableCommand<ReplSetStepDownArgument, Empty> {

  private static final String COMMAND_FIELD_NAME = "replSetStepDown";

  private static final BsonInt32 DEFAULT_STEP_DOWN_SECS = DefaultBsonValues.newInt(60);
  public static final ReplSetStepDownCommand INSTANCE = new ReplSetStepDownCommand();

  private ReplSetStepDownCommand() {
    super(COMMAND_FIELD_NAME);
  }

  @Override
  public Class<? extends ReplSetStepDownArgument> getArgClass() {
    return ReplSetStepDownArgument.class;
  }

  @Override
  public boolean canChangeReplicationState() {
    return true;
  }

  @Override
  public ReplSetStepDownArgument unmarshallArg(BsonDocument requestDoc) throws
      TypesMismatchException {
    return ReplSetStepDownArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(ReplSetStepDownArgument request) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Class<? extends Empty> getResultClass() {
    return Empty.class;
  }

  @Override
  public BsonDocument marshallResult(Empty reply) {
    return null;
  }

  @Override
  public Empty unmarshallResult(BsonDocument resultDoc) {
    return Empty.getInstance();
  }

  @Immutable
  public static class ReplSetStepDownArgument {

    private static final BooleanField FORCE_FIELD = new BooleanField("force");
    private static final LongField SECONDARY_CATCH_UP_PERIOD_SECS_FIELD = new LongField(
        "secondaryCatchUpPeriodSecs");

    private final long seconds;
    private final long catchUpPeriodSecs;
    private final boolean force;

    public ReplSetStepDownArgument(long seconds, long catchUpPeriodSecs, boolean force) {
      this.seconds = seconds;
      this.catchUpPeriodSecs = catchUpPeriodSecs;
      this.force = force;
    }

    public ReplSetStepDownArgument(long seconds) {
      this.seconds = seconds;
      this.force = false;
      this.catchUpPeriodSecs = 10;
    }

    public long getSeconds() {
      return seconds;
    }

    public long getCatchUpPeriodSecs() {
      return catchUpPeriodSecs;
    }

    public boolean isForce() {
      return force;
    }

    private static ReplSetStepDownArgument unmarshall(BsonDocument doc) throws
        TypesMismatchException {
      boolean force = BsonReaderTool.getBoolean(doc, FORCE_FIELD, false);
      long seconds = BsonReaderTool.getNumeric(
          doc,
          COMMAND_FIELD_NAME,
          DEFAULT_STEP_DOWN_SECS)
          .longValue();

      long defaultSecondaryCatchUpPeriodSecs;
      if (force) {
        defaultSecondaryCatchUpPeriodSecs = 0;
      } else {
        defaultSecondaryCatchUpPeriodSecs = 10;
      }
      long catchUpPeriodSecs = BsonReaderTool.getLong(
          doc,
          SECONDARY_CATCH_UP_PERIOD_SECS_FIELD,
          defaultSecondaryCatchUpPeriodSecs
      );

      return new ReplSetStepDownArgument(seconds, catchUpPeriodSecs, force);
    }
  }

}
