
package com.torodb.core.transaction.metainf;

import java.util.Iterator;

/**
 *
 */
public abstract class AbstractMetaDocPartIndex implements MetaDocPartIndex {

    private final String identifier;
    private final boolean unique;

    public AbstractMetaDocPartIndex(String identifier,  boolean unique) {
        this.identifier = identifier;
        this.unique = unique;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public boolean hasSameColumns(MetaDocPartIndex docPartIndex) {
        return hasSameColumns(docPartIndex, iteratorColumns());
    }

    protected boolean hasSameColumns(MetaDocPartIndex docPartIndex, Iterator<? extends MetaDocPartIndexColumn> columnsIterator) {
        Iterator<? extends MetaDocPartIndexColumn> docPartIndexColumnsIterator = docPartIndex.iteratorColumns();
        
        while (columnsIterator.hasNext() && docPartIndexColumnsIterator.hasNext()) {
            MetaDocPartIndexColumn column = columnsIterator.next();
            MetaDocPartIndexColumn docPartIndexColumn = docPartIndexColumnsIterator.next();
            if (!column.getIdentifier().equals(docPartIndexColumn.getIdentifier()) ||
                    column.getOrdering() != docPartIndexColumn.getOrdering()) {
                return false;
            }
        }
        
        return !columnsIterator.hasNext() &&
                !docPartIndexColumnsIterator.hasNext();
    }
    
    @Override
    public String toString() {
        return defautToString();
    }

}
