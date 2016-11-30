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

package com.torodb.torod.pipeline.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.inject.assistedinject.Assisted;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.dsl.backend.BackendTransactionJob;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.pipeline.D2RTranslationBatchFunction;
import com.torodb.torod.pipeline.DefaultToBackendFunction;
import com.torodb.torod.pipeline.InsertPipeline;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.UncheckedException;

import java.util.stream.Stream;

import javax.inject.Inject;

/**
 *
 */
public class SameThreadInsertPipeline implements InsertPipeline {

  private final D2RTranslatorFactory translatorFactory;
  private final MetaDatabase metaDb;
  private final MutableMetaCollection mutableMetaCollection;
  private final WriteBackendTransaction backendConnection;
  private final BackendTransactionJobFactory jobFactory;
  private int docBatchSize = 100;

  @Inject
  public SameThreadInsertPipeline(@Assisted D2RTranslatorFactory translatorFactory,
      @Assisted MetaDatabase metaDb, @Assisted MutableMetaCollection mutableMetaCollection,
      @Assisted WriteBackendTransaction backendConnection, BackendTransactionJobFactory factory) {
    this.translatorFactory = translatorFactory;
    this.metaDb = metaDb;
    this.mutableMetaCollection = mutableMetaCollection;
    this.backendConnection = backendConnection;
    this.jobFactory = factory;
  }

  @Override
  public void insert(Stream<KvDocument> docs) throws RollbackException, UserException {
    D2RTranslationBatchFunction d2rFun =
        new D2RTranslationBatchFunction(translatorFactory, metaDb, mutableMetaCollection);
    DefaultToBackendFunction r2BackendFun =
        new DefaultToBackendFunction(jobFactory, metaDb, mutableMetaCollection);

    try {
      Iterators.partition(docs.iterator(), docBatchSize)
          .forEachRemaining(list -> {
            CollectionData collData = d2rFun.apply(list);
            Iterable<BackendTransactionJob> jobs = r2BackendFun.apply(collData);
            jobs.forEach(Unchecked.consumer(job -> job.execute(backendConnection)));
          });
    } catch (UncheckedException ex) {
      Throwable cause = ex.getCause();
      if (cause != null && cause instanceof UserException) {
        throw (UserException) cause;
      }
      throw ex;
    }
  }

  @Override
  public int getDocsBatchLength() {
    return docBatchSize;
  }

  @Override
  public void setDocsBatchLength(int newBatchLength) {
    Preconditions.checkArgument(newBatchLength > 0,
        "The new batch size must be higher than 0, but %s was recived", newBatchLength);
    this.docBatchSize = newBatchLength;
  }

  public static interface Factory {

    SameThreadInsertPipeline create(
        D2RTranslatorFactory translatorFactory,
        MetaDatabase metaDb,
        MutableMetaCollection mutableMetaCollection,
        WriteBackendTransaction backendConnection);
  }

}
