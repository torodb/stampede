
package com.torodb.poc.query.dsl.mongo;

/**
 *
 */
public interface MongoQuery {

    public <R, A> R visit(MongoQueryVisitor<R, A> visitor, A arg);

}
