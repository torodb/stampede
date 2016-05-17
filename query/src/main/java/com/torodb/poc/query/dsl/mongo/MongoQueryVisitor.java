
package com.torodb.poc.query.dsl.mongo;

/**
 *
 */
<R, A>public interface MongoQueryVisitor<R, A> {

    public R visit(AllMongoQuery mq, A arg);

    public R visit(AndMongoQuery mq, A arg);

    public R visit(ElemMatchMongoQuery mq, A arg);

    public R visit(XorMongoQuery mq, A arg);

    public R visit(IsTypeMongoQuery mq, A arg);

    public R visit(SizeMongoQuery mq, A arg);

    public R visit(SimpleAttOpMongoQuery mq, A arg);

    public R visit(OrMongoQuery mq, A arg);

    public R visit(NotMongoQuery mq, A arg);

    public R visit(IsNumericTypeMongoQuery mq, A arg);

    public R visit(IsArrayTypeMongoQuery mq, A arg);

}
