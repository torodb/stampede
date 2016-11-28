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

package com.torodb.backend;

import com.torodb.core.backend.MetaInfoKey;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvString;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.KvValueAdaptor;
import com.torodb.kvdocument.values.heap.StringKvString;
import org.jooq.DSLContext;

import java.util.Optional;

import javax.inject.Inject;

public class KvMetainfoHandler {

  private static final Serializer SERIALIZER = new Serializer();
  private final SqlInterface sqlInterface;

  @Inject
  public KvMetainfoHandler(SqlInterface sqlInterface) {
    this.sqlInterface = sqlInterface;
  }

  Optional<KvValue<?>> readMetaInfo(DSLContext dsl, MetaInfoKey key) {
    MetaDataReadInterface metaReadI = sqlInterface.getMetaDataReadInterface();

    return metaReadI.readKv(dsl, key)
        .map(this::fromStorableString);
  }

  KvValue<?> writeMetaInfo(DSLContext dsl, MetaInfoKey key, KvValue<?> newValue) {
    String storableString = toStorableString(newValue);
    String storedString = sqlInterface.getMetaDataWriteInterface()
        .writeMetaInfo(dsl, key, storableString);
    if (storedString == null) {
      return null;
    }
    return fromStorableString(storedString);
  }

  private KvValue<?> fromStorableString(String value) {
    switch (value) {
      case "true":
        return KvBoolean.TRUE;
      case "false":
        return KvBoolean.FALSE;
      case "null":
        return KvNull.getInstance();
      default:
    }

    if (value.isEmpty()) {
      return new StringKvString(value);
    }
    char c = value.charAt(0);
    if (c < '0' || c > '9') {
      return new StringKvString(value);
    }
    try {
      int i = Integer.parseInt(value);
      return KvInteger.of(i);
    } catch (NumberFormatException ignore) {
      //just try another conversion
    }
    try {
      long l = Long.parseLong(value);
      return KvLong.of(l);
    } catch (NumberFormatException ignore) {
      //just try another conversion
    }
    try {
      double d = Double.parseDouble(value);
      return KvDouble.of(d);
    } catch (NumberFormatException ignore) {
      //just try another conversion
    }
    return new StringKvString(value);
  }

  private String toStorableString(KvValue<?> value) {
    return value.accept(SERIALIZER, null);
  }

  private static class Serializer extends KvValueAdaptor<String, Void> {

    @Override
    public String defaultCase(KvValue<?> value, Void arg) {
      //TODO: Support all kind of kv values
      throw new UnsupportedOperationException(value.getType()
          + " is not supported as metainf value yet.");
    }

    @Override
    public String visit(KvString value, Void arg) {
      return value.toString();
    }

    @Override
    public String visit(KvDouble value, Void arg) {
      return Double.toString(value.getValue());
    }

    @Override
    public String visit(KvLong value, Void arg) {
      return Long.toString(value.getValue());
    }

    @Override
    public String visit(KvInteger value, Void arg) {
      return Integer.toString(value.getValue());
    }

    @Override
    public String visit(KvNull value, Void arg) {
      return "null";
    }

    @Override
    public String visit(KvBoolean value, Void arg) {
      return Boolean.toString(value.getValue());
    }

  }

}
