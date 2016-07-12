/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.mongodb.language.update;

import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.mongodb.language.Constants;
import com.torodb.kvdocument.values.KVDocument.DocEntry;

/**
 *
 */
public class SetDocumentUpdateAction extends UpdateAction {

    private final KVDocument newValue;

    public SetDocumentUpdateAction(KVDocument newValue) {
        super();
        this.newValue = newValue;
    }

    public KVDocument getNewValue() {
        return newValue;
    }

    @Override
    public void apply(UpdatedToroDocumentBuilder builder) {
        KVValue<?> objectId = null;
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
}
