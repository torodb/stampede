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

package com.torodb.core.transaction.metainf;

import static org.junit.Assert.assertEquals;

import com.torodb.kvdocument.types.*;
import org.junit.Test;

/**
 *
 * @author gortiz
 */
public class FieldTypeTest {

  @Test
  public void testFromSimple() {
    testFromType(BinaryType.INSTANCE, FieldType.BINARY);
    testFromType(BooleanType.INSTANCE, FieldType.BOOLEAN);
    testFromType(DateType.INSTANCE, FieldType.DATE);
    testFromType(DoubleType.INSTANCE, FieldType.DOUBLE);
    testFromType(InstantType.INSTANCE, FieldType.INSTANT);
    testFromType(IntegerType.INSTANCE, FieldType.INTEGER);
    testFromType(LongType.INSTANCE, FieldType.LONG);
    testFromType(MongoObjectIdType.INSTANCE, FieldType.MONGO_OBJECT_ID);
    testFromType(MongoTimestampType.INSTANCE, FieldType.MONGO_TIME_STAMP);
    testFromType(NullType.INSTANCE, FieldType.NULL);
    testFromType(StringType.INSTANCE, FieldType.STRING);
    testFromType(TimeType.INSTANCE, FieldType.TIME);

    testFromType(DocumentType.INSTANCE, FieldType.CHILD);
  }

  @Test
  public void testFromArray() {
    testFromType(new ArrayType(GenericType.INSTANCE), FieldType.CHILD);
    testFromType(new ArrayType(DocumentType.INSTANCE), FieldType.CHILD);
    testFromType(new ArrayType(DoubleType.INSTANCE), FieldType.CHILD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFromGeneric() {
    FieldType.from(GenericType.INSTANCE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFromNonExistent() {
    FieldType.from(NonExistentType.INSTANCE);
  }

  private void testFromType(KvType type, FieldType expectedResult) {
    assertEquals(expectedResult, FieldType.from(type));
  }

}
