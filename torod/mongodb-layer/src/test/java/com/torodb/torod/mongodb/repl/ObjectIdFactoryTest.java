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
 * along with mongodb-layer. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 *
 */
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.bson.BsonObjectId;
import org.junit.Test;

/**
 *
 */
public class ObjectIdFactoryTest {

    public ObjectIdFactoryTest() {
    }

    @Test
    public void testConsumeObjectId() {
        ObjectIdFactory factory = new ObjectIdFactory();

        BsonObjectId oid1 = factory.consumeObjectId();
        BsonObjectId oid2 = factory.consumeObjectId();

        assert !oid1.equals(oid2) : oid1 + " and " + oid2 + " are equal";
    }

}
