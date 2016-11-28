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

package com.torodb.mongodb.repl.commands.impl;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.language.AttributeReference;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand.DropIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand.DropIndexesResult;
import com.torodb.mongodb.language.Constants;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.SharedWriteTorodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class DropIndexesReplImpl extends ReplCommandImpl<DropIndexesArgument, DropIndexesResult> {

  private static final Logger LOGGER =
      LogManager.getLogger(DropIndexesReplImpl.class);

  @Override
  public Status<DropIndexesResult> apply(
      Request req,
      Command<? super DropIndexesArgument, ? super DropIndexesResult> command,
      DropIndexesArgument arg,
      SharedWriteTorodTransaction trans) {
    int indexesBefore = (int) trans.getIndexesInfo(req.getDatabase(), arg.getCollection()).count();

    List<String> indexesToDrop;

    if (!arg.isDropAllIndexes()) {
      if (!arg.isDropByKeys()) {
        if (Constants.ID_INDEX.equals(arg.getIndexToDrop())) {
          LOGGER.warn("Trying to drop index {}. Ignoring the whole request",
              arg.getIndexToDrop());
          return Status.ok(new DropIndexesResult(indexesBefore));
        }
        indexesToDrop = Arrays.asList(arg.getIndexToDrop());
      } else {
        indexesToDrop = trans.getIndexesInfo(req.getDatabase(), arg.getCollection())
            .filter(index -> indexFieldsMatchKeys(index, arg.getKeys()))
            .map(index -> index.getName())
            .collect(Collectors.toList());

        if (indexesToDrop.isEmpty()) {
          LOGGER.warn("Index not found with keys [" + arg.getKeys()
              .stream()
              .map(key -> '"' + key.getKeys()
                  .stream()
                  .collect(Collectors.joining(".")) + "\" :" + key.getType().getName())
              .collect(Collectors.joining(", ")) + "]. Ignoring the whole request",
              arg.getIndexToDrop());
          return Status.ok(new DropIndexesResult(indexesBefore));
        }
      }
    } else {
      indexesToDrop = trans.getIndexesInfo(req.getDatabase(), arg.getCollection())
          .filter(indexInfo -> !Constants.ID_INDEX.equals(indexInfo.getName()))
          .map(indexInfo -> indexInfo.getName())
          .collect(Collectors.toList());
    }

    for (String indexToDrop : indexesToDrop) {
      LOGGER.info("Dropping index {} on collection {}.{}", req.getDatabase(), arg.getCollection(),
          indexToDrop);

      boolean dropped = trans.dropIndex(req.getDatabase(), arg.getCollection(), indexToDrop
      );
      if (!dropped) {
        LOGGER.info("Trying to drop index {}, but it has not been "
            + "found. This is normal since the index could have been filtered or "
            + "we are reapplying oplog during a recovery. Ignoring it", indexToDrop);
      }
    }

    return Status.ok(new DropIndexesResult(indexesBefore));
  }

  private boolean indexFieldsMatchKeys(IndexInfo index, List<IndexOptions.Key> keys) {
    if (index.getFields().size() != keys.size()) {
      return false;
    }

    Iterator<IndexFieldInfo> fieldsIterator = index.getFields().iterator();
    Iterator<IndexOptions.Key> keysIterator = keys.iterator();
    while (fieldsIterator.hasNext()) {
      IndexFieldInfo field = fieldsIterator.next();
      IndexOptions.Key key = keysIterator.next();

      if ((field.isAscending() && key.getType() != KnownType.asc.getIndexType()) || (!field
          .isAscending() && key.getType() != KnownType.desc.getIndexType()) || (field
          .getAttributeReference().getKeys().size() != key.getKeys().size())) {
        return false;
      }

      Iterator<AttributeReference.Key<?>> fieldKeysIterator = field.getAttributeReference()
          .getKeys().iterator();
      Iterator<String> keyKeysIterator = key.getKeys().iterator();

      while (fieldKeysIterator.hasNext()) {
        AttributeReference.Key<?> fieldKey = fieldKeysIterator.next();
        String keyKey = keyKeysIterator.next();

        if (!fieldKey.toString().equals(keyKey)) {
          return false;
        }
      }
    }

    return true;
  }

}
