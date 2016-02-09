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

package com.torodb.torod.core.subdocument.values.heap;

import com.torodb.torod.core.subdocument.values.ScalarMongoTimestamp;

/**
 *
 */
public class DefaultScalarMongoTimestamp extends ScalarMongoTimestamp {

    private static final long serialVersionUID = 5122627079701775584L;

    private final int seconds;
    private final int ordinal;

    public DefaultScalarMongoTimestamp(int seconds, int ordinal) {
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
