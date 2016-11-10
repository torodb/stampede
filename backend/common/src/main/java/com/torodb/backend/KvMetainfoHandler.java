/*
 * MongoWP - ToroDB-poc: Backend common
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend;

import com.torodb.core.backend.MetaInfoKey;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.heap.StringKVString;
import java.util.Optional;
import javax.inject.Inject;
import org.jooq.DSLContext;


/**
 *
 */
public class KvMetainfoHandler {
    private static final Serializer SERIALIZER = new Serializer();
    private final SqlInterface sqlInterface;

    @Inject
    public KvMetainfoHandler(SqlInterface sqlInterface) {
        this.sqlInterface = sqlInterface;
    }

    Optional<KVValue<?>> readMetaInfo(DSLContext dsl, MetaInfoKey key) {
        MetaDataReadInterface metaReadI = sqlInterface.getMetaDataReadInterface();

        return metaReadI.readKV(dsl, key)
                .map(this::fromStorableString);
    }

    KVValue<?> writeMetaInfo(DSLContext dsl, MetaInfoKey key, KVValue<?> newValue) {
        String storableString = toStorableString(newValue);
        String storedString = sqlInterface.getMetaDataWriteInterface()
                .writeMetaInfo(dsl, key, storableString);
        if (storedString == null) {
            return null;
        }
        return fromStorableString(storedString);
    }

    private KVValue<?> fromStorableString(String value) {
        switch (value) {
            case "true": return KVBoolean.TRUE;
            case "false": return KVBoolean.FALSE;
            case "null": return KVNull.getInstance();
        }

        if (value.isEmpty()) {
            return new StringKVString(value);
        }
        char c = value.charAt(0);
        if (c < '0' || c > '9') {
            return new StringKVString(value);
        }
        try {
            int i = Integer.parseInt(value);
            return KVInteger.of(i);
        } catch (NumberFormatException ignore) {
        }
        try {
            long l = Long.parseLong(value);
            return KVLong.of(l);
        } catch (NumberFormatException ignore) {
        }
        try {
            double d = Double.parseDouble(value);
            return KVDouble.of(d);
        } catch (NumberFormatException ex) {
        }
        return new StringKVString(value);
    }

    private String toStorableString(KVValue<?> value) {
        return value.accept(SERIALIZER, null);
    }

    private static class Serializer extends KVValueAdaptor<String, Void> {

        @Override
        public String defaultCase(KVValue<?> value, Void arg) {
            //TODO: Support all kind of kv values
            throw new UnsupportedOperationException(value.getType()
                    + " is not supported as metainf value yet.");
        }

        @Override
        public String visit(KVString value, Void arg) {
            return value.toString();
        }

        @Override
        public String visit(KVDouble value, Void arg) {
            return Double.toString(value.getValue());
        }

        @Override
        public String visit(KVLong value, Void arg) {
            return Long.toString(value.getValue());
        }

        @Override
        public String visit(KVInteger value, Void arg) {
            return Integer.toString(value.getValue());
        }

        @Override
        public String visit(KVNull value, Void arg) {
            return "null";
        }

        @Override
        public String visit(KVBoolean value, Void arg) {
            return Boolean.toString(value.getValue());
        }

    }

}
