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

package com.torodb.mongodb.repl.oplogreplier;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

/**
 * This class test that filters are correctly applied by {@link DefaultOplogApplier}.
 */
public class FilterOplogApplierTest extends DefaultOplogApplierTest {

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return loadData(Lists.newArrayList(
        "create_collection_filtered",
        "dropDatabase_ignored",
        "drop_collection_filtered",
        "rename_collection_filtered_1",
        "rename_collection_filtered_2"
    ));
  }

  @Test
  public void test() throws Exception {
    super.test();
  }

}
