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

import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.torod.pipeline.InsertPipeline;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.torod.pipeline.impl.SameThreadInsertPipeline.Factory;

import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;

/**
 *
 */
public class DefaultInsertPipelineFactory extends IdleTorodbService
    implements InsertPipelineFactory {

  private final AkkaInsertPipelineFactory akkaInsertPipelineFactory;
  private final SameThreadInsertPipeline.Factory sameThreadFactory;

  @Inject
  public DefaultInsertPipelineFactory(ThreadFactory threadFactory,
      AkkaInsertPipelineFactory akkaInsertPipelineFactory,
      Factory sameThreadFactory) {
    super(threadFactory);
    this.akkaInsertPipelineFactory = akkaInsertPipelineFactory;
    this.sameThreadFactory = sameThreadFactory;
  }

  @Override
  protected void startUp() throws Exception {
    akkaInsertPipelineFactory.startAsync();
    akkaInsertPipelineFactory.awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    akkaInsertPipelineFactory.stopAsync();
    akkaInsertPipelineFactory.awaitTerminated();
  }

  @Override
  public InsertPipeline createInsertPipeline(D2RTranslatorFactory translatorFactory,
      MetaDatabase metaDb, MutableMetaCollection mutableMetaCollection,
      WriteBackendTransaction backendConnection, boolean concurrent) {
    if (concurrent) {
      return akkaInsertPipelineFactory.createInsertPipeline(
          translatorFactory, metaDb, mutableMetaCollection,
          backendConnection, concurrent);
    } else {
      return sameThreadFactory.create(translatorFactory, metaDb,
          mutableMetaCollection, backendConnection);
    }
  }

}
