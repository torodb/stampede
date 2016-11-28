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

import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.ArrayField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.ImmutableList;
import com.torodb.mongodb.commands.pojos.OplogOperationParser;
import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand.ApplyOpsArgument;
import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand.ApplyOpsReply;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

public class ApplyOpsCommand extends AbstractNotAliasableCommand<ApplyOpsArgument, ApplyOpsReply> {

  public static final ApplyOpsCommand INSTANCE = new ApplyOpsCommand();

  private ApplyOpsCommand() {
    super("applyOps");
  }

  @Override
  public boolean isSlaveOk() {
    return false;
  }

  @Override
  public Class<? extends ApplyOpsArgument> getArgClass() {
    return ApplyOpsArgument.class;
  }

  @Override
  public ApplyOpsArgument unmarshallArg(BsonDocument requestDoc)
      throws BadValueException, TypesMismatchException, NoSuchKeyException {
    return ApplyOpsArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(ApplyOpsArgument request) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @Override
  public Class<? extends ApplyOpsReply> getResultClass() {
    return ApplyOpsReply.class;
  }

  @Override
  public BsonDocument marshallResult(ApplyOpsReply reply) {
    return reply.marshall();
  }

  @Override
  public ApplyOpsReply unmarshallResult(BsonDocument resultDoc) throws
      MongoException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @Immutable
  public static class ApplyOpsArgument {

    private final boolean alwaysUpsert;
    private final ImmutableList<OplogOperation> operations;
    private final ImmutableList<Precondition> preconditions;

    public ApplyOpsArgument(
        boolean alwaysUpsert,
        ImmutableList<OplogOperation> operations,
        ImmutableList<Precondition> preconditions) {
      this.alwaysUpsert = alwaysUpsert;
      this.operations = operations;
      this.preconditions = preconditions;
    }

    public boolean isAlwaysUpsert() {
      return alwaysUpsert;
    }

    public ImmutableList<OplogOperation> getOperations() {
      return operations;
    }

    public ImmutableList<Precondition> getPreconditions() {
      return preconditions;
    }

    private static ApplyOpsArgument unmarshall(BsonDocument requestDoc)
        throws BadValueException, TypesMismatchException, NoSuchKeyException {
      final String commandName = "applyOps";
      if (!requestDoc.containsKey(commandName) || !requestDoc.get(commandName).isArray()) {
        throw new BadValueException("ops has to be an array");
      }

      boolean alwaysUpsert = BsonReaderTool.getBoolean(requestDoc, "alwaysUpsert", true);

      BsonArray opsArray = requestDoc.get(commandName).asArray();
      ImmutableList.Builder<OplogOperation> ops = ImmutableList.builder();
      for (BsonValue uncastedOp : opsArray) {
        ops.add(OplogOperationParser.fromBson(uncastedOp));
      }

      ImmutableList.Builder<Precondition> preconditions = ImmutableList.builder();
      if (requestDoc.containsKey("preCondition") && requestDoc.get("preCondition").isArray()) {
        for (BsonValue uncastedPrecondition : requestDoc.get("preCondition").asArray()) {
          preconditions.add(Precondition.fromBson(uncastedPrecondition));
        }
      }
      return new ApplyOpsArgument(alwaysUpsert, ops.build(), preconditions.build());
    }

    public static class Precondition {

      private final String namespace;
      private final BsonDocument query;
      private final BsonDocument restriction;

      public Precondition(String namespace, BsonDocument query, BsonDocument restriction) {
        this.namespace = namespace;
        this.query = query;
        this.restriction = restriction;
      }

      public String getNamespace() {
        return namespace;
      }

      public BsonDocument getQuery() {
        return query;
      }

      public BsonDocument getRestriction() {
        return restriction;
      }

      private static Precondition fromBson(BsonValue uncastedPrecondition) throws BadValueException,
          TypesMismatchException, NoSuchKeyException {
        if (!uncastedPrecondition.isDocument()) {
          throw new BadValueException("applyOps preconditions must "
              + "be an array of documents, but one of their "
              + "elements has the non document value '" + uncastedPrecondition);
        }
        BsonDocument preconditionDoc = uncastedPrecondition.asDocument();
        String namespace = BsonReaderTool.getString(preconditionDoc, "ns");
        BsonDocument query = BsonReaderTool.getDocument(preconditionDoc, "q");
        BsonDocument req = BsonReaderTool.getDocument(preconditionDoc, "res");

        return new Precondition(namespace, query, req);
      }
    }

  }

  @Immutable
  public static class ApplyOpsReply {

    private static final DocField GOT_FIELD = new DocField("got");
    private static final DocField WHAT_FAILED_FIELD = new DocField("whatFailed");
    private static final StringField ERRMSG_FIELD = new StringField("errmsg");
    private static final IntField APPLIED_FIELD = new IntField("applied");
    private static final ArrayField RESULT_FIELD = new ArrayField("result");

    private final int num;
    private final ImmutableList<Boolean> results;
    private final BsonDocument got;
    private final BsonDocument whatFailed;

    private ApplyOpsReply(
        BsonDocument got,
        BsonDocument whatFailed) {

      this.got = got;
      this.whatFailed = whatFailed;

      this.num = 0;
      this.results = ImmutableList.of();
    }

    public ApplyOpsReply(int num, ImmutableList<Boolean> results) {
      this.num = num;
      this.results = results;

      this.got = null;
      this.whatFailed = null;
    }

    public int getNum() {
      return num;
    }

    public ImmutableList<Boolean> getResults() {
      return results;
    }

    @Nullable
    public BsonDocument getGot() {
      return got;
    }

    @Nullable
    public BsonDocument getWhatFailed() {
      return whatFailed;
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();
      if (got != null) {
        builder.append(GOT_FIELD, got)
            .append(WHAT_FAILED_FIELD, whatFailed)
            .append(ERRMSG_FIELD, "pre-condition failed");
      } else {
        builder.append(APPLIED_FIELD, getNum());

        BsonArrayBuilder bsonResult = new BsonArrayBuilder();
        for (Boolean iestResult : results) {
          bsonResult.add(iestResult);
        }
        builder.append(RESULT_FIELD, bsonResult.build());
      }
      return builder.build();
    }
  }

}
