
package com.torodb.poc.query;

import com.google.common.collect.Multimap;
import com.torodb.poc.query.dsl.mongo.*;
import java.util.List;
import java.util.function.Function;
import org.jooq.Condition;

/**
 *
 */
public class QueryTranslator implements Function<MongoQuery, Multimap<String, Condition>> {

    @Override
    public Multimap<String, Condition> apply(MongoQuery t) {

    }

    private static class TranslatorVisitor implements MongoQueryVisitor<Void, List<Multimap<String, Condition>>> {

        @Override
        public Void visit(AllMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(AndMongoQuery mq, List<Multimap<String, Condition>> arg) {
            mq.getQ1().visit(this, arg);
            mq.getQ2().visit(this, arg);

            return null;
        }

        @Override
        public Void visit(ElemMatchMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(XorMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(IsTypeMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(SizeMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(SimpleAttOpMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(OrMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(NotMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(IsNumericTypeMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Void visit(IsArrayTypeMongoQuery mq, List<Multimap<String, Condition>> arg) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }
}
