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

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.server.api.oplog.DbCmdOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DbOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DeleteOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.NoopOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperationType;
import com.eightkdata.mongowp.server.api.oplog.OplogVersion;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.eightkdata.mongowp.utils.BsonReaderTool;

import java.util.Locale;
import java.util.function.Function;

import javax.annotation.Nonnull;

public class OplogOperationParser {

  private OplogOperationParser() {
  }

  public static Function<BsonDocument, OplogOperation> asFunction() {
    return AsFunction.INSTANCE;
  }

  public static OplogOperation fromBson(@Nonnull BsonValue uncastedOp) throws
      BadValueException, TypesMismatchException, NoSuchKeyException {
    if (!uncastedOp.isDocument()) {
      throw new BadValueException("found a "
          + uncastedOp.getType().toString().toLowerCase(Locale.ROOT)
          + " where a document that represents a oplog operation "
          + "was expected");
    }
    BsonDocument doc = uncastedOp.asDocument();

    OplogOperationType opType;
    String opString = BsonReaderTool.getString(doc, "op");
    try {
      opType = OplogOperationType.fromOplogName(opString);
    } catch (IllegalArgumentException ex) {
      throw new BadValueException("Unknown oplog operation with type '" + opString + "'");
    }

    String ns;
    try {
      ns = BsonReaderTool.getString(doc, "ns");
    } catch (NoSuchKeyException ex) {
      throw new NoSuchKeyException("ns", "op does not contain required \"ns\" field: "
          + uncastedOp);
    } catch (TypesMismatchException ex) {
      throw ex.newWithMessage("\"ns\" field is not a string: " + uncastedOp);
    }

    if (ns.isEmpty() && !opType.equals(OplogOperationType.NOOP)) {
      throw new BadValueException("\"ns\" field value cannot be empty "
          + "when op type is not 'n': " + doc);
    }
    String db;
    String collection;
    int firstDotIndex = ns.indexOf('.');
    if (firstDotIndex == -1 || firstDotIndex + 1 == ns.length()) {
      db = ns;
      collection = null;
    } else {
      db = ns.substring(0, firstDotIndex);
      collection = ns.substring(firstDotIndex + 1);
    }

    OpTime optime = OpTime.fromOplogEntry(doc);
    long h = BsonReaderTool.getLong(doc, "h");
    OplogVersion version = OplogVersion.valueOf(BsonReaderTool.getInteger(doc, "v"));
    //Note: Mongodb v3 checks if the key exists or not, but doesn't check the value
    boolean fromMigrate = doc.containsKey("fromMigrate");
    BsonDocument o = BsonReaderTool.getDocument(doc, "o");

    switch (opType) {
      case DB:
        return new DbOplogOperation(
            db,
            optime,
            h,
            version,
            fromMigrate
        );
      case DB_CMD:
        return new DbCmdOplogOperation(
            o,
            db,
            optime,
            h,
            version,
            fromMigrate
        );
      case DELETE:
        return new DeleteOplogOperation(
            o,
            db,
            collection,
            optime,
            h,
            version,
            fromMigrate,
            BsonReaderTool.getBoolean(doc, "b", false)
        );
      case INSERT:
        //TODO: parse b
        return new InsertOplogOperation(
            o,
            db,
            collection,
            optime,
            h,
            version,
            fromMigrate
        );
      case NOOP:
        return new NoopOplogOperation(o, db, optime, h, version, fromMigrate);
      case UPDATE:
        return new UpdateOplogOperation(
            BsonReaderTool.getDocument(doc, "o2"),
            db,
            collection,
            optime,
            h,
            version,
            fromMigrate,
            o,
            BsonReaderTool.getBoolean(doc, "b", false)
        );
      default:
        throw new AssertionError(OplogOperationParser.class
            + " is not prepared to work with oplog operations of type " + opType);
    }
  }

  private static class AsFunction implements Function<BsonDocument, OplogOperation> {

    private static final AsFunction INSTANCE = new AsFunction();

    @Override
    public OplogOperation apply(BsonDocument input) {
      if (input == null) {
        return null;
      }
      try {
        return fromBson(input);
      } catch (BadValueException ex) {
        throw new IllegalArgumentException(ex);
      } catch (TypesMismatchException ex) {
        throw new IllegalArgumentException(ex);
      } catch (NoSuchKeyException ex) {
        throw new IllegalArgumentException(ex);
      }
    }

  }
}
