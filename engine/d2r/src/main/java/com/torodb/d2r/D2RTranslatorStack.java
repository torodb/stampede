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

package com.torodb.d2r;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KvDocument;

public class D2RTranslatorStack implements D2RTranslator {

  private final CollectionMetaInfo collectionMetaInfo;
  private final DocPartDataCollection docPartDataCollection;
  private final D2Relational d2Relational;

  @Inject
  public D2RTranslatorStack(TableRefFactory tableRefFactory, IdentifierFactory identifierFactory,
      ReservedIdGenerator ridGenerator, @Assisted MetaDatabase database,
      @Assisted MutableMetaCollection collection) {
    this.collectionMetaInfo = new CollectionMetaInfo(database, collection, identifierFactory,
        ridGenerator);
    this.docPartDataCollection = new DocPartDataCollection(collectionMetaInfo);
    this.d2Relational = new D2Relational(tableRefFactory, docPartDataCollection);
  }

  @Override
  public void translate(KvDocument doc) {
    d2Relational.translate(doc);
  }

  @Override
  public CollectionData getCollectionDataAccumulator() {
    return docPartDataCollection;
  }

}
