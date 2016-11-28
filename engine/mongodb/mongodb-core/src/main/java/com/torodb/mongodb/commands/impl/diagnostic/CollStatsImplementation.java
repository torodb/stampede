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

package com.torodb.mongodb.commands.impl.diagnostic;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.model.NamedToroIndex;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand.CollStatsArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand.CollStatsReply;
import com.torodb.mongodb.core.MongodTransaction;
import com.torodb.mongodb.language.utils.NamespaceUtil;
import com.torodb.torod.CollectionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class CollStatsImplementation implements
    ReadTorodbCommandImpl<CollStatsArgument, CollStatsReply> {

  private static final Logger LOGGER =
      LogManager.getLogger(CollStatsImplementation.class);

  @Override
  public Status<CollStatsReply> apply(Request req,
      Command<? super CollStatsArgument, ? super CollStatsReply> command,
      CollStatsArgument arg, MongodTransaction context) {

    String collection = arg.getCollection();
    CollStatsReply.Builder replyBuilder = new CollStatsReply.Builder(
        req.getDatabase(),
        collection
    );
    if (NamespaceUtil.isSystem(collection)) {
      //TODO (matteom): support stats on system collections
      LOGGER.warn("Requested stats on the system collection "
          + collection + ". ToroDB does not support stats for system "
          + "collections yet");
      Stream<CollectionInfo> collectionsInfo = context.getTorodTransaction().getCollectionsInfo(
          req.getDatabase());
      replyBuilder.setCount(collectionsInfo.count())
          .setSize(0)
          .setStorageSize(0)
          .setCustomStorageStats(null)
          .setIndexDetails(DefaultBsonValues.EMPTY_DOC)
          .setScale(arg.getScale())
          .setSizeByIndex(Collections.<String, Number>emptyMap())
          .setCapped(false);
    } else {
      try {
        CollectionInfo collectionInfo = context.getTorodTransaction().getCollectionInfo(
            req.getDatabase(), arg.getCollection());
        if (collectionInfo.isCapped()) {
          replyBuilder.setCapped(true)
              .setMaxIfCapped(collectionInfo.getMaxIfCapped());
        } else {
          replyBuilder.setCapped(false);
        }
      } catch (CollectionNotFoundException e) {
        //Nothing to do if the collection does not exist
      }
      replyBuilder
          .setCapped(false)
          .setScale(arg.getScale());

      int scale = replyBuilder.getScale();
      //TODO (matteom): add index stats
      Collection<? extends NamedToroIndex> indexes =
          ImmutableList.of();
      Map<String, Long> sizeByMap = Maps.newHashMapWithExpectedSize(indexes.size());

      replyBuilder.setSizeByIndex(sizeByMap);

      replyBuilder.setCount(
          context.getTorodTransaction().countAll(
              req.getDatabase(), collection
          )
      );
      replyBuilder.setSize(
          context.getTorodTransaction().getDocumentsSize(
              req.getDatabase(), collection
          ) / scale
      );
      replyBuilder.setStorageSize(
          context.getTorodTransaction().getCollectionSize(
              req.getDatabase(), collection
          ) / scale
      );
    }

    return Status.ok(replyBuilder.build());
  }

}
