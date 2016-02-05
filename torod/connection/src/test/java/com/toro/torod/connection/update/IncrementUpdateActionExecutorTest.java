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

package com.toro.torod.connection.update;

import com.google.common.collect.Lists;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import org.junit.*;


/**
 *
 */
public class IncrementUpdateActionExecutorTest {
    
    public IncrementUpdateActionExecutorTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testNormal() {
        KVDocumentBuilder builder = KVDocumentBuilder.create();
        builder.newObject("f1")
                .newArray("f2")
                .setValue(3, KVInteger.of(3));
        
        IncrementUpdateActionExecutor.increment(
                new ObjectBuilderCallback(builder), 
                new AttributeReference(Lists.<AttributeReference.Key>newArrayList(
                        new AttributeReference.ObjectKey("f1"),
                        new AttributeReference.ObjectKey("f2"),
                        new AttributeReference.ArrayKey(3)
                )),
                KVInteger.of(1)
        );
        
        assert builder.getObjectBuilder("f1").getArrayBuilder("f2").getValue(3)
                .equals(KVDouble.of(4)) :
                builder.getObjectBuilder("f1").getArrayBuilder("f2").getValue(3)
                + " != 4";
    }

    @Test
    public void testNonExistingAttribute() {
        KVDocumentBuilder builder = KVDocumentBuilder.create();
        builder.newObject("f1")
                .newArray("f2")
                .setValue(3, KVInteger.of(3));
        
        IncrementUpdateActionExecutor.increment(
                new ObjectBuilderCallback(builder), 
                new AttributeReference(Lists.<AttributeReference.Key>newArrayList(
                        new AttributeReference.ObjectKey("fake1"),
                        new AttributeReference.ObjectKey("fake2"),
                        new AttributeReference.ArrayKey(2)
                )),
                KVInteger.of(1)
        );
        
        assert builder.getObjectBuilder("f1").getArrayBuilder("f2").getValue(3)
                .equals(KVInteger.of(3)) :
                builder.getObjectBuilder("f1").getArrayBuilder("f2").getValue(3)
                + " != 3";
        assert builder.contains("fake1");
        assert builder.getObjectBuilder("fake1").getArrayBuilder("fake2").getValue(2)
                .equals(KVInteger.of(1))
                : ".fake != 1";
        
    }

    @Test(expected = UserToroException.class)
    public void testNullAttribute1() {
        KVDocumentBuilder builder = KVDocumentBuilder.create();
        builder.newObject("f1")
                .newArray("f2")
                .setValue(3, KVInteger.of(3));
        
        IncrementUpdateActionExecutor.increment(
                new ObjectBuilderCallback(builder), 
                new AttributeReference(Lists.<AttributeReference.Key>newArrayList(
                        new AttributeReference.ObjectKey("f1"),
                        new AttributeReference.ObjectKey("f2"),
                        new AttributeReference.ArrayKey(2)
                )),
                KVInteger.of(1)
        );
        
    }

    @Test(expected = UserToroException.class)
    public void testIllegalPath() {
        KVDocumentBuilder builder = KVDocumentBuilder.create();
        builder.newObject("f1")
                .newArray("f2")
                .setValue(3, KVInteger.of(3));
        
        IncrementUpdateActionExecutor.increment(
                new ObjectBuilderCallback(builder), 
                new AttributeReference(Lists.<AttributeReference.Key>newArrayList(
                        new AttributeReference.ObjectKey("f1"),
                        new AttributeReference.ArrayKey(2),
                        new AttributeReference.ArrayKey(2)
                )),
                KVInteger.of(1)
        );
    }

    
}
