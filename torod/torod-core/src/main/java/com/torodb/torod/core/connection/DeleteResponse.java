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

package com.torodb.torod.core.connection;

import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 *
 */
public class DeleteResponse {
    
    private final boolean success;
    private final long deleted;
    private final Collection<WriteError> errors;

    public DeleteResponse(boolean success, long deleted, @Nullable Collection<WriteError> errors) {
        this.success = success;
        this.deleted = deleted;
        if (errors == null) {
            this.errors = Collections.emptyList();
        } else {
            this.errors = Collections.unmodifiableCollection(errors);
        }
    }

    public boolean isSuccess() {
        return success;
    }

    public long getDeleted() {
        return deleted;
    }

    public Collection<WriteError> getErrors() {
        return errors;
    }
    
    public JsonObject toJson() {
        JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("success", success)
                .add("deletedSize", deleted);
        
        if (!errors.isEmpty()) {
            JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
            for (WriteError writeError : errors) {
                arrayBuilder.add(writeError.toJson());
            }
            builder.add("errors", arrayBuilder.build());
        }
        
        return builder.build();
    }

    @Override
    public String toString() {
        return toJson().toString();
    }
    
}
