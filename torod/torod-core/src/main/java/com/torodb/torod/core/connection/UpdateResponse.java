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

import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class UpdateResponse {
    private final int candidates;
    private final int modified;
    private final Collection<InsertedDocuments> insertedDocuments;
    private final Collection<WriteError> errors;

    public UpdateResponse(int candidates, int modified, Collection<InsertedDocuments> insertedDocuments, Collection<WriteError> errors) {
        this.candidates = candidates;
        this.modified = modified;
        this.insertedDocuments = insertedDocuments;
        this.errors = errors;
    }

    public boolean isSuccess() {
        return errors.isEmpty();
    }

    public int getCandidates() {
        return candidates;
    }

    public int getModified() {
        return modified;
    }

    public Collection<InsertedDocuments> getInsertedDocuments() {
        return insertedDocuments;
    }

    public Collection<WriteError> getErrors() {
        return errors;
    }

    @Override
    public String toString() {
        return "UpdateResponse{" + "candidates=" + candidates + ", modified=" +
                modified + ", insertedDocuments=" + insertedDocuments +
                ", errors=" + errors + '}';
    }
    
    public static class Builder {
        private int candidates;
        private int modified;
        private final Collection<InsertedDocuments> insertedDocuments = Lists.newArrayList();
        private final List<WriteError> errors = Lists.newArrayList();
        private boolean built = false;

        public Builder setCandidates(int candidates) {
            if (built) {
                throw new IllegalStateException("This builder cannot be reused");
            }
            this.candidates = candidates;
            return this;
        }
        
        public Builder addCandidates(int delta) {
            if (built) {
                throw new IllegalStateException("This builder cannot be reused");
            }
            this.candidates += delta;
            return this;
        }

        public Builder setModified(int modified) {
            if (built) {
                throw new IllegalStateException("This builder cannot be reused");
            }
            this.modified = modified;
            return this;
        }
        
        public Builder incrementModified(int delta) {
            if (built) {
                throw new IllegalStateException("This builder cannot be reused");
            }
            this.modified += delta;
            return this;
        }

        public Builder addInsertedDocument(int docId, int operationIndex) {
            if (built) {
                throw new IllegalStateException("This builder cannot be reused");
            }
            insertedDocuments.add(new InsertedDocuments(docId, operationIndex));
            return this;
        }
        
        public Builder addError(WriteError error) {
            this.errors.add(error);
            return this;
        }
        
        public UpdateResponse build() {
            if (built) {
                throw new IllegalStateException("This builder cannot be reused");
            }
            built = true;
            return new UpdateResponse(candidates, modified, insertedDocuments, errors);
        }
    }

    public static class InsertedDocuments {

        private final int operationIndex;
        private final int docId;

        private InsertedDocuments(int docId, int operationIndex) {
            this.operationIndex = operationIndex;
            this.docId = docId;
        }

        public int getOperationIndex() {
            return operationIndex;
        }

        public int getDocId() {
            return docId;
        }
    }
    
}
