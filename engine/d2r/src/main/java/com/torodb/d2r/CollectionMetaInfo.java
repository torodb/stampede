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

import com.torodb.core.TableRef;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.d2r.ReservedIdGenerator.DocPartRidGenerator;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

public class CollectionMetaInfo {

  private final MetaDatabase metaDatabase;
  private final MutableMetaCollection metaCollection;
  private final IdentifierFactory identifierFactory;
  private final DocPartRidGenerator docPartRidGenerator;

  public CollectionMetaInfo(MetaDatabase metaDatabase, MutableMetaCollection metaCollection,
      IdentifierFactory identifierFactory, ReservedIdGenerator ridGenerator) {
    this.metaDatabase = metaDatabase;
    this.metaCollection = metaCollection;
    this.identifierFactory = identifierFactory;
    this.docPartRidGenerator = ridGenerator.getDocPartRidGenerator(metaDatabase.getName(),
        metaCollection.getName());
  }

  public int getNextRowId(TableRef tableRef) {
    return docPartRidGenerator.nextRid(tableRef);
  }

  public MutableMetaDocPart findMetaDocPart(TableRef tableRef) {
    MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
    if (metaDocPart == null) {
      String docPartIdentifier = identifierFactory.toDocPartIdentifier(metaDatabase, metaCollection
          .getName(), tableRef);
      metaDocPart = metaCollection.addMetaDocPart(tableRef, docPartIdentifier);
    }
    return metaDocPart;
  }

  public String getFieldIdentifier(TableRef tableRef, FieldType fieldType, String field) {
    MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
    return identifierFactory.toFieldIdentifier(metaDocPart, field, fieldType);
  }

  public String getScalarIdentifier(FieldType fieldType) {
    return identifierFactory.toFieldIdentifierForScalar(fieldType);
  }

}
