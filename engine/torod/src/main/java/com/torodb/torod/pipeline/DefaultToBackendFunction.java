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

package com.torodb.torod.pipeline;

import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.BackendTransactionJob;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.function.Function;

/**
 *
 */
public class DefaultToBackendFunction implements
    Function<CollectionData, Iterable<BackendTransactionJob>> {

  private static final Logger LOGGER = LogManager.getLogger(DefaultToBackendFunction.class);

  private final BackendTransactionJobFactory factory;
  private final MetaDatabase database;
  private final MetaCollection collection;

  public DefaultToBackendFunction(BackendTransactionJobFactory factory, MetaDatabase database,
      MetaCollection collection) {
    this.factory = factory;
    this.database = database;
    this.collection = collection;
  }

  public Iterable<BackendTransactionJob> apply(CollectionData collectionData) {
    ArrayList<BackendTransactionJob> jobs = new ArrayList<>();
    for (DocPartData docPartData : collectionData.orderedDocPartData()) {
      assert docPartData.getMetaDocPart() instanceof BatchMetaDocPart :
          "This function can only use inputs whose meta doc part information is an instance of "
          + BatchMetaDocPart.class;
      BatchMetaDocPart metaDocPart = (BatchMetaDocPart) docPartData.getMetaDocPart();
      if (metaDocPart.isCreatedOnCurrentBatch()) {
        jobs.add(factory.createAddDocPartDdlJob(database, collection, metaDocPart));
        metaDocPart.streamScalars()
            .map((scalar) -> factory.createAddScalarDdlJob(database, collection, metaDocPart,
                scalar))
            .forEachOrdered((job) -> jobs.add(job));
        metaDocPart.streamFields()
            .map((field) -> factory.createAddFieldDdlJob(database, collection, metaDocPart, field))
            .forEachOrdered((job) -> jobs.add(job));
      } else {
        //it already exists, we only need to add the new scalars and fields
        for (ImmutableMetaScalar newScalar : metaDocPart.getOnBatchModifiedMetaScalars()) {
          jobs.add(factory.createAddScalarDdlJob(database, collection, metaDocPart, newScalar));
        }
        for (ImmutableMetaField newField : metaDocPart.getOnBatchModifiedMetaFields()) {
          jobs.add(factory.createAddFieldDdlJob(database, collection, metaDocPart, newField));
        }
      }

      jobs.add(factory.insert(database, collection, docPartData));
    }

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Executing the following jobs: {}", jobs);
    }

    return jobs;
  }

}
