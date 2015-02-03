
package com.torodb.torod.core.exceptions;

/**
 *
 */
public class ExistentIndexException extends UserToroException {
    private static final long serialVersionUID = 1L;

    private final String collectionName;
    private final String indexName;

    public ExistentIndexException(String collectionName, String indexName) {
        super("Index " + indexName +" already exists");
        this.collectionName = collectionName;
        this.indexName = indexName;
    }

    public ExistentIndexException(String collectionName, String indexName, String message) {
        super(message);
        this.collectionName = collectionName;
        this.indexName = indexName;
    }
    
}
