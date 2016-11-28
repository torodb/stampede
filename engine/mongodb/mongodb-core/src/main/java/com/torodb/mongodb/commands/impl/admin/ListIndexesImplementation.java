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

package com.torodb.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.commands.pojos.CursorResult;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.IndexVersion;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.commands.pojos.index.type.IndexType;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand.ListIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand.ListIndexesResult;
import com.torodb.mongodb.core.MongodTransaction;
import com.torodb.torod.IndexFieldInfo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class ListIndexesImplementation implements
    ReadTorodbCommandImpl<ListIndexesArgument, ListIndexesResult> {

  @Override
  public Status<ListIndexesResult> apply(Request req,
      Command<? super ListIndexesArgument, ? super ListIndexesResult> command,
      ListIndexesArgument arg, MongodTransaction context) {
    return Status.ok(new ListIndexesResult(
        CursorResult.createSingleBatchCursor(req.getDatabase(), arg.getCollection(),
            context.getTorodTransaction().getIndexesInfo(req.getDatabase(), arg.getCollection())
                .map(indexInfo ->
                    new IndexOptions(
                        IndexVersion.V1,
                        indexInfo.getName(),
                        req.getDatabase(),
                        arg.getCollection(),
                        false,
                        indexInfo.isUnique(),
                        false,
                        0,
                        indexInfo.getFields().stream()
                            .map(field -> new IndexOptions.Key(extractKeys(field), extractType(
                                field)))
                            .collect(Collectors.toList()),
                        null,
                        null)
                )
        )
    ));
  }

  public <T, K, U> Collector<T, ?, LinkedHashMap<K, U>> toMap(
      Function<? super T, ? extends K> keyMapper,
      Function<? super T, ? extends U> valueMapper) {
    return Collectors.toMap(keyMapper, valueMapper, throwingMerger(), LinkedHashMap::new);
  }

  private static <T> BinaryOperator<T> throwingMerger() {
    return (u, v) -> {
      throw new IllegalStateException(String.format("Duplicate key %s", u));
    };
  }

  private List<String> extractKeys(IndexFieldInfo indexFieldInfo) {
    return indexFieldInfo.getAttributeReference().getKeys().stream()
        .map(k -> k.getKeyValue().toString()).collect(Collectors.toList());
  }

  private IndexType extractType(IndexFieldInfo indexFieldInfo) {
    return indexFieldInfo.isAscending() ? KnownType.asc.getIndexType() : KnownType.desc
        .getIndexType();
  }

}
