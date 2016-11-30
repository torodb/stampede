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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.torodb.core.exceptions.user.UnsupportedCompoundIndexException;
import com.torodb.core.exceptions.user.UnsupportedUniqueIndexException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.commands.pojos.index.type.AscIndexType;
import com.torodb.mongodb.commands.pojos.index.type.DefaultIndexTypeVisitor;
import com.torodb.mongodb.commands.pojos.index.type.DescIndexType;
import com.torodb.mongodb.commands.pojos.index.type.IndexType;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesResult;
import com.torodb.mongodb.language.Constants;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.SharedWriteTorodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class CreateIndexesReplImpl
    extends ReplCommandImpl<CreateIndexesArgument, CreateIndexesResult> {

  private static final Logger LOGGER =
      LogManager.getLogger(CreateIndexesReplImpl.class);

  @SuppressWarnings("checkstyle:LineLength")
  private static final FieldIndexOrderingConverterIndexTypeVisitor filedIndexOrderingConverterVisitor =
      new FieldIndexOrderingConverterIndexTypeVisitor();

  private final ReplicationFilters replicationFilters;

  @Inject
  public CreateIndexesReplImpl(ReplicationFilters replicationFilters) {
    this.replicationFilters = replicationFilters;
  }

  @Override
  public Status<CreateIndexesResult> apply(Request req,
      Command<? super CreateIndexesArgument, ? super CreateIndexesResult> command,
      CreateIndexesArgument arg, SharedWriteTorodTransaction trans) {
    int indexesBefore = (int) trans.getIndexesInfo(req.getDatabase(), arg.getCollection()).count();
    int indexesAfter = indexesBefore;

    try {
      boolean existsCollection = trans.existsCollection(req.getDatabase(), arg.getCollection());
      final boolean createdCollectionAutomatically = !existsCollection;

      if (!existsCollection) {
        LOGGER.info("Creating collection {} on {}.{}", req.getDatabase(), arg.getCollection());

        trans.createIndex(req.getDatabase(), arg.getCollection(), Constants.ID_INDEX,
            ImmutableList.<IndexFieldInfo>of(new IndexFieldInfo(new AttributeReference(Arrays
                .asList(new Key[]{new ObjectKey(Constants.ID)})), FieldIndexOrdering.ASC
                .isAscending())), true);
      }

      for (IndexOptions indexOptions : arg.getIndexesToCreate()) {
        if (!replicationFilters.getIndexPredicate().test(req.getDatabase(), arg.getCollection(),
            indexOptions.getName(), indexOptions.isUnique(), indexOptions.getKeys())) {
          LOGGER.info("Skipping filtered index {}.{}.{}.",
              req.getDatabase(), arg.getCollection(), indexOptions.getName());
          continue;
        }

        if (indexOptions.getKeys().size() < 1) {
          return Status.from(ErrorCode.CANNOT_CREATE_INDEX, "Index keys cannot be empty.");
        }

        if (indexOptions.isBackground()) {
          LOGGER.info("Building index in background is not supported. Ignoring option");
        }

        if (indexOptions.isSparse()) {
          LOGGER.info("Sparse index are not supported. Ignoring option");
        }

        boolean skipIndex = false;
        List<IndexFieldInfo> fields = new ArrayList<>(indexOptions.getKeys().size());
        for (IndexOptions.Key indexKey : indexOptions.getKeys()) {
          AttributeReference.Builder attRefBuilder = new AttributeReference.Builder();
          for (String key : indexKey.getKeys()) {
            attRefBuilder.addObjectKey(key);
          }

          IndexType indexType = indexKey.getType();

          if (!KnownType.contains(indexType)) {
            String note = "Bad index key pattern: Unknown index type '"
                + indexKey.getType().getName() + "'. Skipping index.";
            LOGGER.info(note);
            skipIndex = true;
            break;
          }

          Optional<FieldIndexOrdering> ordering = indexType.accept(
              filedIndexOrderingConverterVisitor, null);
          if (!ordering.isPresent()) {
            String note = "Index of type " + indexType.getName()
                + " is not supported. Skipping index.";
            LOGGER.info(note);
            skipIndex = true;
            break;
          }

          fields.add(new IndexFieldInfo(attRefBuilder.build(), ordering.get().isAscending()));
        }

        if (skipIndex) {
          continue;
        }

        try {
          LOGGER.info("Creating index {} on collection {}.{}", req.getDatabase(), arg
              .getCollection(), indexOptions.getName());

          if (trans.createIndex(req.getDatabase(), arg.getCollection(), indexOptions.getName(),
              fields, indexOptions.isUnique())) {
            indexesAfter++;
          }
        } catch (UnsupportedCompoundIndexException ex) {
          String note =
              "Compound indexes are not supported. Skipping index.";
          LOGGER.info(note);
          continue;
        } catch (UnsupportedUniqueIndexException ex) {
          String note =
              "Unique index with keys on distinct subdocuments is not supported. Skipping index.";
          LOGGER.info(note);
          continue;
        }
      }

      String note = null;

      if (indexesAfter == indexesBefore) {
        note = "all indexes already exist";
      }

      return Status.ok(new CreateIndexesResult(indexesBefore, indexesAfter, note,
          createdCollectionAutomatically));
    } catch (UserException ex) {
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
    }
  }

  private static class FieldIndexOrderingConverterIndexTypeVisitor
      extends DefaultIndexTypeVisitor<Void, Optional<FieldIndexOrdering>> {

    @Override
    protected Optional<FieldIndexOrdering> defaultVisit(IndexType indexType, Void arg) {
      return Optional.empty();
    }

    @Override
    public Optional<FieldIndexOrdering> visit(AscIndexType indexType, Void arg) {
      return Optional.of(FieldIndexOrdering.ASC);
    }

    @Override
    public Optional<FieldIndexOrdering> visit(DescIndexType indexType, Void arg) {
      return Optional.of(FieldIndexOrdering.DESC);
    }
  }

}
