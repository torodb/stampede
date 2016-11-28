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

package com.torodb.mongodb.language.update;

import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvDocument.DocEntry;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.language.Constants;

/**
 *
 */
public class SetDocumentUpdateAction extends UpdateAction {

  private final KvDocument newValue;

  public SetDocumentUpdateAction(KvDocument newValue) {
    super();
    this.newValue = newValue;
  }

  public KvDocument getNewValue() {
    return newValue;
  }

  @Override
  public void apply(UpdatedToroDocumentBuilder builder) {
    KvValue<?> objectId = null;
    if (builder.contains(Constants.ID)) {
      objectId = builder.getValue(Constants.ID);
    }

    builder.clear();
    for (DocEntry<?> entry : newValue) {
      builder.putValue(entry.getKey(), entry.getValue());
    }

    if (objectId != null) {
      builder.putValue(Constants.ID, objectId);
    }

    builder.setUpdated();
  }

  @Override
  public boolean isSetModification() {
    return true;
  }

  @Override
  public <R, A> R accept(UpdateActionVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }
}
