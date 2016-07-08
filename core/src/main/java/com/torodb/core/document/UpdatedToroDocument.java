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

package com.torodb.core.document;

import com.torodb.kvdocument.values.KVDocument;

public class UpdatedToroDocument extends ToroDocument {

    private final boolean hasDid;
    private final boolean updated;
    
    public UpdatedToroDocument(int id, KVDocument root, boolean updated) {
        super(id, root);
        
        this.hasDid = true;
        this.updated = updated;
    }
    
    public UpdatedToroDocument(KVDocument root, boolean updated) {
        super(-1, root);
        
        this.hasDid = false;
        this.updated = updated;
    }

    public boolean isUpdated() {
        return updated;
    }

    public boolean hasDid() {
        return hasDid;
    }
}
