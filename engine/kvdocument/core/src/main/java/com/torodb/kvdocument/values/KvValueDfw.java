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

package com.torodb.kvdocument.values;

import com.torodb.kvdocument.values.KvDocument.DocEntry;

/**
 *
 * @param <A> The auxiliary argument of the pre and post methods. If it is not needed,
 *              {@link java.lang.Void} can be used
 */
@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public class KvValueDfw<A> implements KvValueVisitor<Void, A> {

  protected void preKvValue(KvValue<?> value, A arg) {
  }

  protected void postKvValue(KvValue<?> value, A arg) {
  }

  protected void preInt(KvInteger value, A arg) {
  }

  protected void postInt(KvInteger value, A arg) {
  }

  @Override
  public Void visit(KvInteger value, A arg) {
    preKvValue(value, arg);
    preInt(value, arg);

    postInt(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preLong(KvLong value, A arg) {
  }

  protected void postLong(KvLong value, A arg) {
  }

  @Override
  public Void visit(KvLong value, A arg) {
    preKvValue(value, arg);
    preLong(value, arg);

    postLong(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preString(KvString value, A arg) {
  }

  protected void postString(KvString value, A arg) {
  }

  @Override
  public Void visit(KvString value, A arg) {
    preKvValue(value, arg);
    preString(value, arg);

    postString(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preDouble(KvDouble value, A arg) {
  }

  protected void postDouble(KvDouble value, A arg) {
  }

  @Override
  public Void visit(KvDouble value, A arg) {
    preKvValue(value, arg);
    preDouble(value, arg);

    postDouble(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preDoc(KvDocument value, A arg) {
  }

  protected void postDoc(KvDocument value, A arg) {
  }

  @Override
  public Void visit(KvDocument value, A arg) {
    preKvValue(value, arg);
    preDoc(value, arg);

    for (DocEntry<?> entry : value) {
      entry.getValue().accept(this, arg);
    }

    postDoc(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preBoolean(KvBoolean value, A arg) {
  }

  protected void postBoolean(KvBoolean value, A arg) {
  }

  @Override
  public Void visit(KvBoolean value, A arg) {
    preKvValue(value, arg);
    preBoolean(value, arg);

    postBoolean(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preNull(KvNull value, A arg) {
  }

  protected void postNull(KvNull value, A arg) {
  }

  @Override
  public Void visit(KvNull value, A arg) {
    preKvValue(value, arg);
    preNull(value, arg);

    postNull(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preArray(KvArray value, A arg) {
  }

  protected void postArray(KvArray value, A arg) {
  }

  @Override
  public Void visit(KvArray value, A arg) {
    preKvValue(value, arg);
    preArray(value, arg);

    for (KvValue<?> element : value) {
      element.accept(this, arg);
    }

    postArray(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preMongoObjectId(KvMongoObjectId value, A arg) {
  }

  protected void postMongoObjectId(KvMongoObjectId value, A arg) {
  }

  @Override
  public Void visit(KvMongoObjectId value, A arg) {

    preKvValue(value, arg);
    preMongoObjectId(value, arg);

    postMongoObjectId(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preDateTime(KvInstant value, A arg) {
  }

  protected void postDateTime(KvInstant value, A arg) {
  }

  @Override
  public Void visit(KvInstant value, A arg) {

    preKvValue(value, arg);
    preDateTime(value, arg);

    postDateTime(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preDate(KvDate value, A arg) {
  }

  protected void postDate(KvDate value, A arg) {
  }

  @Override
  public Void visit(KvDate value, A arg) {

    preKvValue(value, arg);
    preDate(value, arg);

    postDate(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preTime(KvTime value, A arg) {
  }

  protected void postTime(KvTime value, A arg) {
  }

  @Override
  public Void visit(KvTime value, A arg) {

    preKvValue(value, arg);
    preTime(value, arg);

    postTime(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preBinary(KvBinary value, A arg) {
  }

  protected void postBinary(KvBinary value, A arg) {
  }

  @Override
  public Void visit(KvBinary value, A arg) {
    preKvValue(value, arg);
    preBinary(value, arg);

    postBinary(value, arg);
    postKvValue(value, arg);

    return null;
  }

  protected void preMongoTimestamp(KvMongoTimestamp value, A arg) {
  }

  protected void postMongoTimestamp(KvMongoTimestamp value, A arg) {
  }

  @Override
  public Void visit(KvMongoTimestamp value, A arg) {
    preKvValue(value, arg);
    preMongoTimestamp(value, arg);

    postMongoTimestamp(value, arg);
    postKvValue(value, arg);

    return null;
  }

}
