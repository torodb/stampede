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

package com.torodb.poc.backend;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.torodb.torod.core.subdocument.structure.DocStructure;

/**
 *
 */
@Immutable
public class SplitDocument {

    private final int id;
    private final DocStructure root;
    private final Multimap<String, PathDocument> pathDocuments;

    SplitDocument(
            int id,
            @Nonnull DocStructure structure,
            @Nonnull Multimap<String, PathDocument> pathDocuments) {
        this.id = id;
        this.root = structure;
        this.pathDocuments = Multimaps.unmodifiableMultimap(pathDocuments);
    }

    public DocStructure getRoot() {
        return root;
    }

    public int getDocumentId() {
        return id;
    }

    public Multimap<String, PathDocument> getPathDocuments() {
        return pathDocuments;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 29 * hash + this.id;
        hash = 29 * hash + (this.root != null ? this.root.hashCode() : 0);
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
        final SplitDocument other = (SplitDocument) obj;
        if (this.id != other.id) {
            return false;
        }
        if (this.root != other.root && (this.root == null ||
                !this.root.equals(other.root))) {
            return false;
        }
        if (this.pathDocuments != other.pathDocuments &&
                (this.pathDocuments == null ||
                !this.pathDocuments.equals(other.pathDocuments))) {
            return false;
        }
        return true;
    }

    public static class Builder {

        private int id;
        private DocStructure root;
        private final Multimap<String, PathDocument> subDocuments;
        private boolean build;

        public Builder() {
            subDocuments = HashMultimap.create();
            build = false;
        }

        public DocStructure getStructure() {
            Preconditions.checkState(!build, "This builder has already been used");
            return root;
        }

        public Builder setId(int id) {
            Preconditions.checkState(!build, "This builder has already been used");
            this.id = id;
            return this;
        }

        public Builder setRoot(DocStructure root) {
            Preconditions.checkState(!build, "This builder has already been used");
            this.root = root;

            return this;
        }

        public Builder add(PathDocument pathDocument) {
            Preconditions.checkState(!build, "This builder has already been used");
            String path = pathDocument.getPath();

            subDocuments.put(path, pathDocument);
            return this;
        }

        public SplitDocument build() {
            Preconditions.checkState(!build, "This builder has already been used");

            if (root == null) {
                throw new IllegalStateException("structure must be non null");
            }
            assert checkCorrectness();

            build = true;

            return new SplitDocument(
                    id,
                    root,
                    subDocuments
            );
        }

        private boolean checkCorrectness() {
            return true;
        }
    }

}
