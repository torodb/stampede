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

import com.torodb.torod.core.subdocument.values.ScalarInstant;
import org.threeten.bp.Instant;

/**
 *
 */
public class LongScalarInstant extends ScalarInstant {

    private static final long serialVersionUID = 5624484392290784071L;

    private final long millis;

    public LongScalarInstant(long millis) {
        this.millis = millis;
    }

    @Override
    public long getMillisFromUnix() {
        return millis;
    }

    @Override
    public Instant getValue() {
        return Instant.ofEpochMilli(millis);
    }

}
