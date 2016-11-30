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

package com.torodb.mongodb.language.update;

import com.google.common.collect.Lists;
import com.torodb.core.exceptions.user.UpdateException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class IncrementUpdateActionTest {

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testNormal() throws UpdateException {
    UpdatedToroDocumentBuilder builder = UpdatedToroDocumentBuilder.create();
    builder.newObject("f1")
        .newArray("f2")
        .setValue(3, KvInteger.of(3));

    new IncrementUpdateAction(
        Lists.<AttributeReference>newArrayList(new AttributeReference(
            Lists.<AttributeReference.Key<?>>newArrayList(
                new AttributeReference.ObjectKey("f1"),
                new AttributeReference.ObjectKey("f2"),
                new AttributeReference.ArrayKey(3)
            ))),
        KvInteger.of(1)
    ).apply(builder);

    assert builder.getObjectBuilder("f1").getArrayBuilder("f2").getValue(3)
        .equals(KvDouble.of(4)) :
        builder.getObjectBuilder("f1").getArrayBuilder("f2").getValue(3)
        + " != 4";
  }

  @Test
  public void testNonExistingAttribute() throws UpdateException {
    UpdatedToroDocumentBuilder builder = UpdatedToroDocumentBuilder.create();
    builder.newObject("f1")
        .newArray("f2")
        .setValue(3, KvInteger.of(3));

    new IncrementUpdateAction(
        Lists.<AttributeReference>newArrayList(new AttributeReference(
            Lists.<AttributeReference.Key<?>>newArrayList(
                new AttributeReference.ObjectKey("fake1"),
                new AttributeReference.ObjectKey("fake2"),
                new AttributeReference.ArrayKey(2)
            ))),
        KvInteger.of(1)
    ).apply(builder);;

    assert builder.getObjectBuilder("f1").getArrayBuilder("f2").getValue(3)
        .equals(KvInteger.of(3)) :
        builder.getObjectBuilder("f1").getArrayBuilder("f2").getValue(3)
        + " != 3";
    assert builder.contains("fake1");
    assert builder.getObjectBuilder("fake1").getArrayBuilder("fake2").getValue(2)
        .equals(KvInteger.of(1)) : ".fake != 1";
  }

  @Test(expected = UserException.class)
  public void testNullAttribute1() throws UpdateException {
    UpdatedToroDocumentBuilder builder = UpdatedToroDocumentBuilder.create();
    builder.newObject("f1")
        .newArray("f2")
        .setValue(3, KvInteger.of(3));

    new IncrementUpdateAction(
        Lists.<AttributeReference>newArrayList(new AttributeReference(
            Lists.<AttributeReference.Key<?>>newArrayList(
                new AttributeReference.ObjectKey("f1"),
                new AttributeReference.ObjectKey("f2"),
                new AttributeReference.ArrayKey(2)
            ))),
        KvInteger.of(1)
    ).apply(builder);;
  }

  @Test(expected = UserException.class)
  public void testIllegalPath() throws UpdateException {
    UpdatedToroDocumentBuilder builder = UpdatedToroDocumentBuilder.create();
    builder.newObject("f1")
        .newArray("f2")
        .setValue(3, KvInteger.of(3));

    new IncrementUpdateAction(
        Lists.<AttributeReference>newArrayList(new AttributeReference(
            Lists.<AttributeReference.Key<?>>newArrayList(
                new AttributeReference.ObjectKey("f1"),
                new AttributeReference.ArrayKey(2),
                new AttributeReference.ArrayKey(2)
            ))),
        KvInteger.of(1)
    ).apply(builder);;
  }

}
