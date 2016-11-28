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

package com.torodb.core.impl;

import com.torodb.core.TableRef;
import org.junit.Assert;
import org.junit.Test;

public class TableRefImplTest {

  private static final TableRefFactoryImpl tableRefFactory = new TableRefFactoryImpl();

  ;

    @Test
  public void rootTest() throws Exception {
    TableRef tableRef = createTableRef();
    Assert.assertEquals("", tableRef.getName());
    Assert.assertEquals(0, tableRef.getDepth());
    Assert.assertEquals(0, tableRef.getArrayDimension());
    Assert.assertEquals(true, tableRef.isRoot());
    Assert.assertEquals(false, tableRef.isInArray());
    Assert.assertEquals(false, tableRef.getParent().isPresent());
  }

  @Test
  public void rootObjectTest() throws Exception {
    TableRef tableRef = createTableRef("object");
    Assert.assertEquals("object", tableRef.getName());
    Assert.assertEquals(1, tableRef.getDepth());
    Assert.assertEquals(0, tableRef.getArrayDimension());
    Assert.assertEquals(false, tableRef.isRoot());
    Assert.assertEquals(false, tableRef.isInArray());
    Assert.assertEquals(true, tableRef.getParent().isPresent());
  }

  @Test
  public void rootObjectObjectTest() throws Exception {
    TableRef tableRef = createTableRef("object", "object");
    Assert.assertEquals("object", tableRef.getName());
    Assert.assertEquals(2, tableRef.getDepth());
    Assert.assertEquals(0, tableRef.getArrayDimension());
    Assert.assertEquals(false, tableRef.isRoot());
    Assert.assertEquals(false, tableRef.isInArray());
    Assert.assertEquals(true, tableRef.getParent().isPresent());
  }

  @Test
  public void rootArrayTest() throws Exception {
    TableRef tableRef = createTableRef("array");
    Assert.assertEquals("array", tableRef.getName());
    Assert.assertEquals(1, tableRef.getDepth());
    Assert.assertEquals(0, tableRef.getArrayDimension());
    Assert.assertEquals(false, tableRef.isRoot());
    Assert.assertEquals(false, tableRef.isInArray());
    Assert.assertEquals(true, tableRef.getParent().isPresent());
  }

  @Test
  public void rootArrayInArrayTest() throws Exception {
    TableRef tableRef = createTableRef("array", "2");
    Assert.assertEquals("$2", tableRef.getName());
    Assert.assertEquals(2, tableRef.getDepth());
    Assert.assertEquals(2, tableRef.getArrayDimension());
    Assert.assertEquals(false, tableRef.isRoot());
    Assert.assertEquals(true, tableRef.isInArray());
    Assert.assertEquals(true, tableRef.getParent().isPresent());
  }

  @Test
  public void rootArrayInArrayInArrayTest() throws Exception {
    TableRef tableRef = createTableRef("array", "2", "3");
    Assert.assertEquals("$3", tableRef.getName());
    Assert.assertEquals(3, tableRef.getDepth());
    Assert.assertEquals(3, tableRef.getArrayDimension());
    Assert.assertEquals(false, tableRef.isRoot());
    Assert.assertEquals(true, tableRef.isInArray());
    Assert.assertEquals(true, tableRef.getParent().isPresent());
  }

  private TableRef createTableRef(String... names) {
    TableRef tableRef = tableRefFactory.createRoot();

    for (String name : names) {
      try {
        int index = Integer.parseInt(name);
        tableRef = tableRefFactory.createChild(tableRef, index);
      } catch (NumberFormatException ex) {
        tableRef = tableRefFactory.createChild(tableRef, name);
      }
    }

    return tableRef;
  }
}
