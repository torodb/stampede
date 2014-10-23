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

package com.torodb.mongowp.mongoserver.api.toro.util;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.torodb.torod.core.subdocument.ToroDocument;

import com.torodb.kvdocument.values.ObjectValue;

/**
 *
 */
public class MongoDocument implements ToroDocument {

    private final ObjectValue root;

    public MongoDocument(ObjectValue root) {
        this.root = root;
    }

    @Override
    public ObjectValue getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return "MongoDocument{doc= " + getRoot() + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + Objects.hashCode(this.root);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MongoDocument other = (MongoDocument) obj;
        if (!Objects.equals(this.root, other.root)) {
            return false;
        }
        return true;
    }
    
    public static class Builder implements ToroDocument.DocumentBuilder {

        private ObjectValue root;

        @Override
        public DocumentBuilder setRoot(ObjectValue root) {
            this.root = root;
            return this;
        }

        @Override
        public ToroDocument build() {
            Preconditions.checkArgument(root != null);
            
            return new MongoDocument(root);
        }

    }

}
