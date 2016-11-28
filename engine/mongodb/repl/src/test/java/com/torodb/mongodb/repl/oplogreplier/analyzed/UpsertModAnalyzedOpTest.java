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

package com.torodb.mongodb.repl.oplogreplier.analyzed;

import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.language.update.SetFieldUpdateAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

/**
 *
 * @author gortiz
 */
public class UpsertModAnalyzedOpTest extends AbstractAnalyzedOpTest<UpsertModAnalyzedOp> {

  private static final Logger LOGGER = LogManager.getLogger(UpsertModAnalyzedOpTest.class);

  @Override
  UpsertModAnalyzedOp getAnalyzedOp(KvValue<?> mongoDocId) {
    return new UpsertModAnalyzedOp(mongoDocId, (oldDoc) -> {
      return UpdateActionsTool.applyModification(//modification is $set: {a: 3}
          oldDoc,
          new SetFieldUpdateAction(
              Collections.singleton(
                  new AttributeReference.Builder().addObjectKey("a").build()),
              KvInteger.of(3)
          )
      );
    });
  }

  @Override
  Logger getLogger() {
    return LOGGER;
  }

  //TODO: Callback checks can be improved
  @Override
  void andThenInsertTest() {
    andThenInsert((colOp, newDoc, oldDoc) -> {
    }, (colOp, newDoc) -> {
    });
  }

  //TODO: Callback checks can be improved
  @Override
  void andThenUpdateModTest() {
    andThenUpdateMod((colOp, newDoc, oldDoc) -> {
    }, (colOp, newDoc) -> {
    });
  }

  //TODO: Callback checks can be improved
  @Override
  void andThenUpdateSetTest() {
    andThenUpdateSet((colOp, newDoc, oldDoc) -> {
    }, (colOp, newDoc) -> {
    });
  }

  //TODO: Callback checks can be improved
  @Override
  void andThenUpsertModTest() {
    andThenUpsertMod((colOp, newDoc, oldDoc) -> {
    }, (colOp, newDoc) -> {
    });
  }

  //TODO: Callback checks can be improved
  @Override
  void andThenUpsertSetTest() {
    andThenUpsertSet((colOp, newDoc, oldDoc) -> {
    }, (colOp, newDoc) -> {
    });
  }

  //TODO: Callback checks can be improved
  @Override
  void andThenDeleteTest() {
    andThenDelete((colOp, newDoc, oldDoc) -> {
    }, (colOp, newDoc) -> {
    });
  }

}
