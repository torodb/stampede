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

package com.torodb.kvdocument.values.heap;

import com.torodb.kvdocument.values.KVMongoTimestamp;

/**
 *
 */
public class DefaultKVMongoTimestamp extends KVMongoTimestamp {

    private static final long serialVersionUID = -1503905785806988728L;

    private final int seconds;
    private final int ordinal;

    public DefaultKVMongoTimestamp(int seconds, int ordinal) {
        this.seconds = seconds;
        this.ordinal = ordinal;
    }

    @Override
    public int getSecondsSinceEpoch() {
        return seconds;
    }

    @Override
    public int getOrdinal() {
        return ordinal;
    }

}
