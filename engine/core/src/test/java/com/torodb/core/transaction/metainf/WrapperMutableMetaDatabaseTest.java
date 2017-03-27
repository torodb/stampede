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


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import java.util.Collections;

public class WrapperMutableMetaDatabaseTest {

  @Test
  public void indexStateOnCollectionDropAndDelete() {
    //GIVEN
    ImmutableMetaIndex originalIndex = new ImmutableMetaIndex("testIndex", true);
    ImmutableMetaCollection originalCol = new ImmutableMetaCollection(
        "testCol",
        "testCol",
        Collections.emptyList(),
        Collections.singletonList(originalIndex)
    );
    ImmutableMetaDatabase originalDb = new ImmutableMetaDatabase(
        "testDb",
        "testDb",
        Collections.singleton(originalCol)
    );
    WrapperMutableMetaDatabase db = new WrapperMutableMetaDatabase(originalDb, (t) -> {});

    //WHEN
    db.removeMetaCollectionByName("testCol");
    db.addMetaCollection("testCol", "testCol");

    //THEN
    WrapperMutableMetaCollection newCol = db.getMetaCollectionByName("testCol");
    assertThat(newCol, is(not(nullValue())));
    WrapperMutableMetaIndex newIndex = newCol.getMetaIndexByName("testIndex");
    assertThat(newIndex, is(nullValue()));
  }

}
