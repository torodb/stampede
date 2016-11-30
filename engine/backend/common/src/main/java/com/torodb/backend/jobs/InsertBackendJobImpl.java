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

import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.InsertBackendJob;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;

public class InsertBackendJobImpl implements InsertBackendJob {

  private final MetaDatabase db;
  private final MetaCollection col;
  private final DocPartData data;

  public InsertBackendJobImpl(MetaDatabase db, MetaCollection col, DocPartData data) {
    super();
    this.db = db;
    this.col = col;
    this.data = data;
  }

  @Override
  public void execute(WriteBackendTransaction connection) throws UserException {
    connection.insert(db, col, data);
  }

  @Override
  public MetaDatabase getDatabase() {
    return db;
  }

  @Override
  public MetaCollection getCollection() {
    return col;
  }

  @Override
  public DocPartData getDataToInsert() {
    return data;
  }

  @Override
  public String toString() {
    MetaDocPart docPart = data.getMetaDocPart();
    int rowCount = data.rowCount();
    return "insert{db:" + db + ", col:" + col + ", docPart:" + docPart + ", rows:" + rowCount + '}';
  }

}
