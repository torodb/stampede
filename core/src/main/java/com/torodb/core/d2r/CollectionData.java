package com.torodb.core.d2r;

public interface CollectionData {
    /**
     * The {@code Iterable<DocPartData>} returned by this method will be ordered 
     * ascending by value of {@code DocPartData.getMetaDocPart().getTableRef().getDepth()}
     */
    public Iterable<DocPartData> orderedDocPartData();
}
