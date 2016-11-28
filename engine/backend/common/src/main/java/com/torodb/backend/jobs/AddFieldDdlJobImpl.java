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
import com.torodb.core.dsl.backend.AddFieldDdlJob;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

public class AddFieldDdlJobImpl implements AddFieldDdlJob {

  private final MetaDatabase db;
  private final MetaCollection col;
  private final MutableMetaDocPart docPart;
  private final MetaField newField;

  public AddFieldDdlJobImpl(MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart,
      MetaField newField) {
    super();
    this.db = db;
    this.col = col;
    this.docPart = docPart;
    this.newField = newField;
  }

  @Override
  public void execute(WriteBackendTransaction connection) throws UserException {
    connection.addField(db, col, docPart, newField);
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
  public MetaField getField() {
    return newField;
  }

  @Override
  public String toString() {
    return "add field{db:" + db + ", col:" + col + ", docPart:" + docPart + ", field:" + newField
        + '}';
  }

}
