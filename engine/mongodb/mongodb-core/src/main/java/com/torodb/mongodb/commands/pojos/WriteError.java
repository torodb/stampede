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

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class WriteError {

  private static final IntField INDEX_FIELD = new IntField("index");
  private static final IntField CODE_FIELD = new IntField("code");
  private static final StringField ERR_MSG_FIELD = new StringField("errmsg");

  private final int index;
  private final int code;
  private final String errMsg;

  public WriteError(int index, int code, String errMsg) {
    this.index = index;
    this.code = code;
    this.errMsg = errMsg;
  }

  public int getIndex() {
    return index;
  }

  public int getCode() {
    return code;
  }

  public String getErrMsg() {
    return errMsg;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 19 * hash + this.index;
    hash = 19 * hash + this.code;
    hash = 19 * hash + (this.errMsg != null ? this.errMsg.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final WriteError other = (WriteError) obj;
    if (this.index != other.index) {
      return false;
    }
    if (this.code != other.code) {
      return false;
    }
    return !((this.errMsg == null) ? (other.errMsg != null) :
        !this.errMsg.equals(other.errMsg));
  }

  public BsonValue<?> marshall() {
    BsonDocumentBuilder bsonWriteError = new BsonDocumentBuilder();
    bsonWriteError.append(INDEX_FIELD, index);
    bsonWriteError.append(CODE_FIELD, code);
    bsonWriteError.append(ERR_MSG_FIELD, errMsg);
    return bsonWriteError.build();
  }

  public static WriteError unmarshall(BsonDocument doc) throws TypesMismatchException,
      NoSuchKeyException {
    return new WriteError(
        BsonReaderTool.getNumeric(doc, INDEX_FIELD).intValue(),
        BsonReaderTool.getNumeric(doc, CODE_FIELD).intValue(),
        BsonReaderTool.getString(doc, ERR_MSG_FIELD, null)
    );
  }

}
