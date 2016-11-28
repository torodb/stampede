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

package com.torodb.backend.jobs;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.AddCollectionDdlJob;
import com.torodb.core.dsl.backend.AddDatabaseDdlJob;
import com.torodb.core.dsl.backend.AddDocPartDdlJob;
import com.torodb.core.dsl.backend.AddFieldDdlJob;
import com.torodb.core.dsl.backend.AddScalarDddlJob;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.dsl.backend.InsertBackendJob;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

public class BackendConnectionJobFactoryImpl implements BackendTransactionJobFactory {

  @Override
  public AddDatabaseDdlJob createAddDatabaseDdlJob(MetaDatabase db) {
    return new AddDatabaseDdlJobImpl(db);
  }

  @Override
  public AddCollectionDdlJob createAddCollectionDdlJob(MetaDatabase db, MetaCollection col) {
    return new AddCollectionDdlJobImpl(db, col);
  }

  @Override
  public AddDocPartDdlJob createAddDocPartDdlJob(MetaDatabase db, MetaCollection col,
      MetaDocPart docPart) {
    return new AddDocPartDdlJobImpl(db, col, docPart);
  }

  @Override
  public AddFieldDdlJob createAddFieldDdlJob(MetaDatabase db, MetaCollection col,
      MutableMetaDocPart docPart,
      MetaField field) {
    return new AddFieldDdlJobImpl(db, col, docPart, field);
  }

  @Override
  public AddScalarDddlJob createAddScalarDdlJob(MetaDatabase db, MetaCollection col,
      MetaDocPart docPart, MetaScalar scalar) {
    return new AddScalarDdlJobImpl(db, col, docPart, scalar);
  }

  @Override
  public InsertBackendJob insert(MetaDatabase db, MetaCollection col, DocPartData data) {
    return new InsertBackendJobImpl(db, col, data);
  }

}
