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

import com.google.inject.Module;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 *
 */
@RunWith(Parameterized.class)
public abstract class AbstractOplogApplierTest {

  @Parameter(0)
  public String testName;
  @Parameter(1)
  public OplogTest oplogTest;
  @Rule
  public OplogTestContextResourceRule testContextResource =
      new OplogTestContextResourceRule(this::getMongodSpecificTestModule);

  public abstract Module getMongodSpecificTestModule();

  protected static Collection<Object[]> loadData(ArrayList<String> filesList) {
    return filesList.stream()
        .map(filename -> Tuple.tuple(filename, filename + ".json"))
        .map(Unchecked.<Tuple2<String, String>, Tuple2<String, OplogTest>>function((
            Tuple2<String, String> tuple) -> {
          try {
            OplogTest test = OplogTestParser.fromExtendedJsonResource(tuple.v2);
            return Tuple.tuple(tuple.v1, test);
          } catch (Throwable ex) {
            throw new AssertionError("Failed to parse '" + tuple.v2 + "'", ex);
          }
        }))
        .map(tuple -> new Object[]{
      tuple.v2.getTestName().orElse(tuple.v1),
      tuple.v2
    })
        .collect(Collectors.toList());
  }

  protected void test() throws Exception {
    Assume.assumeTrue("Test " + this.oplogTest + " marked as ignorable", !oplogTest.shouldIgnore());

    oplogTest.execute(testContextResource.getTestContext());
  }

}
