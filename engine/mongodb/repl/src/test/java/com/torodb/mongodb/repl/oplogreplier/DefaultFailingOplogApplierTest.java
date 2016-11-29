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
import com.torodb.mongodb.repl.oplogreplier.OplogApplier.UnexpectedOplogApplierException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

/**
 *
 */
public class DefaultFailingOplogApplierTest extends DefaultOplogApplierTest {

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return loadData(Lists.newArrayList(
        "applyOps",
        "unknownCommand",
        "emptyCommand",
        "insert_without_id",
        "update_no_upsert_without_id",
        "delete_without_id",
        "insert_doc_id",
        "insert_arr_id",
        "update_doc_id",
        "update_arr_id",
        "delete_doc_id",
        "delete_arr_id"
    ));
  }

  @Test(expected = UnexpectedOplogApplierException.class)
  public void test() throws Exception {
    super.test();
  }

}
