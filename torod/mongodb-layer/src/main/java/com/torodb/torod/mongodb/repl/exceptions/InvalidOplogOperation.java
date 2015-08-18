
package com.torodb.torod.mongodb.repl.exceptions;

import org.bson.BsonDocument;

/**
 *
 */
public class InvalidOplogOperation extends Exception {
    private static final long serialVersionUID = 1L;
    private final BsonDocument doc;

    public InvalidOplogOperation(BsonDocument doc) {
        this.doc = doc;
    }

    public InvalidOplogOperation(BsonDocument doc, String message) {
        super(message);
        this.doc = doc;
    }

    public InvalidOplogOperation(BsonDocument doc, String message, Throwable cause) {
        super(message, cause);
        this.doc = doc;
    }

    public InvalidOplogOperation(BsonDocument doc, Throwable cause) {
        super(cause);
        this.doc = doc;
    }

    public BsonDocument getDoc() {
        return doc;
    }

}
