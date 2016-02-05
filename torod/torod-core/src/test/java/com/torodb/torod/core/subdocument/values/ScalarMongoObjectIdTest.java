/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.core.subdocument.values;

import com.torodb.torod.core.subdocument.values.heap.ByteArrayScalarMongoObjectId;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class ScalarMongoObjectIdTest {

    private static final byte[] SAMPLE_BYTES = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12};

    @Test
    public void testParse() {
        ScalarMongoObjectId original = new ByteArrayScalarMongoObjectId(SAMPLE_BYTES);
        ScalarMongoObjectId copy = new ByteArrayScalarMongoObjectId(original.getArrayValue());

        assertArrayEquals(original.getArrayValue(), SAMPLE_BYTES);
        assertArrayEquals(copy.getArrayValue(), SAMPLE_BYTES);
        assertEquals(copy, original);
    }

}
