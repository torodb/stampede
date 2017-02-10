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
import com.torodb.kvdocument.values.*;

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
  public Void visit(KvJavascript value, DocConsumer arg) {
    return null;
  }

  @Override
  public Void visit(KvJavascriptWithScope value, DocConsumer arg) {
    return null;
  }

}
