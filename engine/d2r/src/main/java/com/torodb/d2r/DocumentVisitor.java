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

package com.torodb.d2r;

import com.torodb.d2r.D2Relational.DocConsumer;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvBinary;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDate;
import com.torodb.kvdocument.values.KvDecimal128;
import com.torodb.kvdocument.values.KvDeprecated;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInstant;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvMaxKey;
import com.torodb.kvdocument.values.KvMinKey;
import com.torodb.kvdocument.values.KvMongoDbPointer;
import com.torodb.kvdocument.values.KvMongoJavascript;
import com.torodb.kvdocument.values.KvMongoJavascriptWithScope;
import com.torodb.kvdocument.values.KvMongoObjectId;
import com.torodb.kvdocument.values.KvMongoRegex;
import com.torodb.kvdocument.values.KvMongoTimestamp;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvString;
import com.torodb.kvdocument.values.KvTime;
import com.torodb.kvdocument.values.KvUndefined;
import com.torodb.kvdocument.values.KvValueVisitor;

public class DocumentVisitor implements KvValueVisitor<Void, DocConsumer> {

  @Override
  public Void visit(KvBoolean value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvNull value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvArray value, DocConsumer arg) {
    arg.consume(value);
    return null;
  }

  @Override
  public Void visit(KvInteger value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvLong value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvDouble value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvString value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvDocument value, DocConsumer arg) {
    arg.consume(value);
    return null;
  }

  @Override
  public Void visit(KvMongoObjectId value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvInstant value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvDate value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvTime value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvBinary value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvMongoTimestamp value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvDecimal128 value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvMongoJavascript value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvMongoJavascriptWithScope value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvMinKey value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvMaxKey value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvUndefined kvUndefined, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvMongoRegex value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvMongoDbPointer value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvDeprecated value, DocConsumer arg) {
    return null;
  }

}
