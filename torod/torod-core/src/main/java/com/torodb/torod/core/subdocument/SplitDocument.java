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

package com.torodb.torod.core.subdocument;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElementDFW;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class SplitDocument {

    private final int id;
    private final DocStructure root;
    /**
     * A table (SubDocType, Index, SubDocument> where index is the position of the given subdocument in this document
     * related to the other subdocuments of the given type.
     * <p>
     * Example: given the following document:      <code>
     * {
     *      "owner" : {
     *          "name": "alice"
     *      },
     *      "quantity": 1000
     *      "beneficiary": {
     *          "name": "bob"
     *      }
     *      "others": [
     *          {
     *              "name": "charlie"
     *          }
     *      ]
     * }
     * </code>
     * <p>
     * Each subdocument with subdocument type (name: string) has a unique index between the other subdocuments of the
     * given type (for example, alice would have the first, bob the second and charlie the third, but the order is
     * <b>NOT</b> defined!). The subdocument whose type is (owner: object, beneficiary: object, others: array,
     * quantity:int) has its own index whose value is one as there is no other subdocument with the this type.
     */
    private final Table<SubDocType, Integer, SubDocument> subDocuments;
    private final Multimap<SubDocType, DocStructure> structures;

    SplitDocument(
            int id,
            @Nonnull DocStructure structure,
            @Nonnull Table<SubDocType, Integer, SubDocument> subDocuments,
            @Nonnull Multimap<SubDocType, DocStructure> structures) {
        this.id = id;
        this.root = structure;
        this.subDocuments = Tables.unmodifiableTable(subDocuments);
        this.structures = Multimaps.unmodifiableMultimap(structures);
    }

    public DocStructure getRoot() {
        return root;
    }

    public int getDocumentId() {
        return id;
    }

    public Table<SubDocType, Integer, SubDocument> getSubDocuments() {
        return subDocuments;
    }

    public Multimap<SubDocType, DocStructure> getStructures() {
        return structures;
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
        if (this.subDocuments != other.subDocuments &&
                (this.subDocuments == null ||
                !this.subDocuments.equals(other.subDocuments))) {
            return false;
        }
        if (this.structures != other.structures &&
                (this.structures == null ||
                !this.structures.equals(other.structures))) {
            return false;
        }
        return true;
    }

    public static class Builder {

        private static final DocIndexer INDEXER = new DocIndexer();

        private int id;
        private DocStructure root;
        private final Table<SubDocType, Integer, SubDocument> subDocuments;
        private final Multimap<SubDocType, DocStructure> structures;
        private final Map<SubDocType, Integer> indexBySubDoc;
        private boolean build;

        public Builder() {
            subDocuments = HashBasedTable.create();
            structures = HashMultimap.create();
            indexBySubDoc = Maps.newHashMap();
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
            structures.clear();
            root.accept(INDEXER, this);

            return this;
        }

        public Builder add(SubDocument subDocument) {
            Preconditions.checkState(!build, "This builder has already been used");
            SubDocType type = subDocument.getType();

            Integer index = indexBySubDoc.get(type);
            if (index == null) {
                index = 0;
            } else {
                index++;
            }
            indexBySubDoc.put(type, index);

            subDocuments.put(type, index, subDocument);
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
                    subDocuments,
                    structures
            );
        }

        private boolean checkCorrectness() {
            Deque<DocStructure> structuresToCheck = new LinkedList<DocStructure>();
            structuresToCheck.add(root);
            while (!structuresToCheck.isEmpty()) {
                DocStructure docStructure = structuresToCheck.removeLast();
                boolean contained = subDocuments.contains(docStructure.getType(), docStructure.getIndex());

                if (!contained) {
                    throw new IllegalStateException("The structure references the subdocument "
                            + docStructure.getType() + " with index " + docStructure.getIndex() + ", which is not "
                            + "contained in the subdocuments table");
                }
            }
            return true;
        }

        private static class DocIndexer extends StructureElementDFW<SplitDocument.Builder> {

            @Override
            protected void postDocStructure(DocStructure structure, Builder arg) {
                arg.structures.put(structure.getType(), structure);
            }

        }
    }

}
