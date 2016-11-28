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
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KvDocument;

import java.util.List;
import java.util.function.Function;

/**
 *
 */
public class D2RTranslationBatchFunction implements Function<List<KvDocument>, CollectionData> {

  private final D2RTranslatorFactory translatorFactory;
  private final MetaDatabase metaDatabase;
  private final BatchMetaCollection metaDocCollection;

  public D2RTranslationBatchFunction(D2RTranslatorFactory translatorFactory,
      MetaDatabase metaDb,
      MutableMetaCollection metaCol) {
    this.translatorFactory = translatorFactory;
    this.metaDatabase = metaDb;
    this.metaDocCollection = createMetaDocCollection(metaCol);
  }

  //For testing purpose
  protected BatchMetaCollection createMetaDocCollection(MutableMetaCollection metaCol) {
    return new BatchMetaCollection(metaCol);
  }

  @Override
  public CollectionData apply(List<KvDocument> docs) {
    metaDocCollection.newBatch();
    D2RTranslator translator = translatorFactory.createTranslator(metaDatabase, metaDocCollection);

    for (KvDocument doc : docs) {
      translator.translate(doc);
    }

    return translator.getCollectionDataAccumulator();
  }

}
