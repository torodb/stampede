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
import com.torodb.core.dsl.backend.AddScalarDddlJob;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaScalar;

public class AddScalarDdlJobImpl implements AddScalarDddlJob {

  private final MetaDatabase db;
  private final MetaCollection col;
  private final MetaDocPart docPart;
  private final MetaScalar newScalar;

  public AddScalarDdlJobImpl(MetaDatabase db, MetaCollection col, MetaDocPart docPart,
      MetaScalar newScalar) {
    this.db = db;
    this.col = col;
    this.docPart = docPart;
    this.newScalar = newScalar;
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
  public MetaDocPart getDocPart() {
    return docPart;
  }

  @Override
  public MetaScalar getScalar() {
    return newScalar;
  }

  @Override
  public void execute(WriteBackendTransaction connection) throws UserException {
    connection.addScalar(db, col, docPart, newScalar);
  }

  @Override
  public String toString() {
    return "add scalar{db:" + db + ", col:" + col + ", docPart:" + docPart + ", scalar:" + newScalar
        + '}';
  }

}
