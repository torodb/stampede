
package com.torodb.poc.query.dsl.mongo;

/**
 *
 */
public class AndMongoQuery implements MongoQuery {

    private final MongoQuery q1;
    private final MongoQuery q2;

    public AndMongoQuery(MongoQuery q1, MongoQuery q2) {
        this.q1 = q1;
        this.q2 = q2;
    }

    public MongoQuery getQ1() {
        return q1;
    }

    public MongoQuery getQ2() {
        return q2;
    }

    @Override
    public <R, A> R visit(MongoQueryVisitor<R, A> visitor, A arg) {
        return visitor.visit(this, arg);
    }
    
}
