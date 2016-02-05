/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with kvdocument-core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.MongoTimestampType;

/**
 *
 */
public abstract class KVMongoTimestamp extends KVValue<KVMongoTimestamp>{

    private static final long serialVersionUID = 6199736068678561291L;

    public abstract int getSecondsSinceEpoch();

    public abstract int getOrdinal();

    @Override
    public KVMongoTimestamp getValue() {
        return this;
    }

    @Override
    public Class<? extends KVMongoTimestamp> getValueClass() {
        return getClass();
    }

    @Override
    public MongoTimestampType getType() {
        return MongoTimestampType.INSTANCE;
    }

    @Override
    public String toString() {
        return "Timestamp{"
                + "seconds:" + getSecondsSinceEpoch()
                + ", ordinal:" + getOrdinal()
                + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof KVMongoTimestamp)) {
            return false;
        }
        KVMongoTimestamp other = (KVMongoTimestamp) obj;
        if (this.getSecondsSinceEpoch() != other.getSecondsSinceEpoch()) {
            return false;
        }
        return this.getOrdinal() == other.getOrdinal();
    }

    @Override
    public int hashCode() {
        return getSecondsSinceEpoch() << 4 | (getOrdinal() & 0xF);
    }

    @Override
    public <Result, Arg> Result accept(KVValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

}
