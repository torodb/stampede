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

package com.torodb.torod.mongodb.commands.impl.general.update;

import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
public class SetDocumentUpdateActionExecutor {
    
    static boolean set(
            MongoUpdatedToroDocumentBuilder builder,
            KVDocument newValue
    ) {
        KVValue<?> objectId = null;
        if (builder.contains("_id")) {
            objectId = builder.getValue("_id");
        }
        
        builder.clear();
        for (DocEntry<?> entry : newValue) {
            builder.putValue(entry.getKey(), entry.getValue());
        }
        
        if (objectId != null) {
            builder.putValue("_id", objectId);
        }
        
        return true;
    }
    
}
