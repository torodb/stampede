
package com.torodb.torod.core.utils;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVString;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class DocValueQueryCriteriaEvaluator {

    private DocValueQueryCriteriaEvaluator()  {}
    
    public static boolean evaluate(@Nonnull QueryCriteria qc, @Nonnull KVValue<?> value) {
        return qc.accept(
                new QueryCriteriaEvaluator(), 
                value
        );
    }
    
    public static Predicate<KVValue<?>> createPredicate(QueryCriteria query) {
        return new QueryCriteriaPredicate(query);
    }
    
    private static class QueryCriteriaPredicate implements Predicate<KVValue<?>> {

        private final QueryCriteria qc;
        private final QueryCriteriaEvaluator evaluator;

        public QueryCriteriaPredicate(@Nonnull QueryCriteria qc) {
            this.qc = qc;
            this.evaluator = new QueryCriteriaEvaluator();
        }
        
        @Override
        public boolean apply(KVValue<?> value) {
            return qc.accept(evaluator, value);
        }
    }

    private static class QueryCriteriaEvaluator
            implements QueryCriteriaVisitor<Boolean, KVValue<?>> {

        @Nullable
        private KVValue<?> resolve(AttributeReference attRef, KVValue<?> value) {
            if (attRef.getKeys().isEmpty()) {
                return value;
            }
            if (value instanceof KVDocument) {
                return resolve(attRef.getKeys().iterator(), (KVDocument) value);
            }
            if (value instanceof KVArray) {
                return resolve(attRef.getKeys().iterator(), (KVArray) value);
            }
            return null;
        }

        @Nullable
        private KVValue<?> resolve(Iterator<AttributeReference.Key> atts, KVDocument value) {
            if (!atts.hasNext()) {
                return value;
            }
            else {
                AttributeReference.Key nextAtt = atts.next();
                if (!(nextAtt instanceof AttributeReference.ObjectKey)) {
                    return null;
                }
                else {
                    AttributeReference.ObjectKey castedAtt
                            = (AttributeReference.ObjectKey) nextAtt;
                    if (!value.containsKey(castedAtt.getKeyValue())) {
                        return null;
                    }
                    KVValue<?> referencedValue
                            = value.get(castedAtt.getKeyValue());
                    if (!atts.hasNext()) {
                        return referencedValue;
                    }
                    if (referencedValue instanceof KVDocument) {
                        return resolve(atts, (KVDocument) referencedValue);
                    }
                    if (referencedValue instanceof KVArray) {
                        return resolve(atts, (KVArray) referencedValue);
                    }
                    else {
                        return null;
                    }
                }
            }
        }

        @Nullable
        private KVValue<?> resolve(Iterator<AttributeReference.Key> atts, KVArray value) {
            if (!atts.hasNext()) {
                return value;
            }
            else {
                AttributeReference.Key nextAtt = atts.next();
                if (!(nextAtt instanceof AttributeReference.ArrayKey)) {
                    return null;
                }
                else {
                    AttributeReference.ArrayKey castedAtt
                            = (AttributeReference.ArrayKey) nextAtt;
                    if (castedAtt.getIndex() < 0) {
                        return null;
                    }
                    if (castedAtt.getIndex() >= value.size()) {
                        return null;
                    }
                    KVValue<?> referencedValue
                            = value.get(castedAtt.getKeyValue());
                    if (!atts.hasNext()) {
                        return referencedValue;
                    }
                    if (referencedValue instanceof KVDocument) {
                        return resolve(atts, (KVDocument) referencedValue);
                    }
                    if (referencedValue instanceof KVArray) {
                        return resolve(atts, (KVArray) referencedValue);
                    }
                    else {
                        return null;
                    }
                }
            }
        }

        @Override
        public Boolean visit(TrueQueryCriteria criteria, KVValue<?> arg) {
            return true;
        }

        @Override
        public Boolean visit(FalseQueryCriteria criteria, KVValue<?> arg) {
            return false;
        }

        @Override
        public Boolean visit(AndQueryCriteria criteria, KVValue<?> arg) {
            return criteria.getSubQueryCriteria1().accept(this, arg)
                    && criteria.getSubQueryCriteria2().accept(this, arg);
        }

        @Override
        public Boolean visit(OrQueryCriteria criteria, KVValue<?> arg) {
            return criteria.getSubQueryCriteria1().accept(this, arg)
                    || criteria.getSubQueryCriteria2().accept(this, arg);
        }

        @Override
        public Boolean visit(NotQueryCriteria criteria, KVValue<?> arg) {
            return !criteria.getSubQueryCriteria().accept(this, arg);
        }

        @Override
        public Boolean visit(TypeIsQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }

            ScalarType basicType = ScalarType.fromDocType(referenced.getType());
            return criteria.getExpectedType().equals(basicType);
        }

        @Override
        public Boolean visit(IsEqualQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }

            return criteria.getValue().equals(KVValueToScalarValue.fromDocValue(referenced));
        }

        @Override
        public Boolean visit(IsGreaterQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            Object innerValue = criteria.getValue().getValue();
            if (innerValue instanceof Number) {
                if (referenced.getValue() instanceof Number) {
                    Number refNumber = (Number) referenced.getValue();
                    return refNumber.doubleValue()
                            > ((Number) innerValue).doubleValue();
                }
                return false;
            }
            if (innerValue instanceof String) {
                if (referenced.getValue() instanceof String) {
                    return ((String) referenced.getValue()).compareTo((String) innerValue)
                            > 0;
                }
                return false;
            }
            return false;
        }

        @Override
        public Boolean visit(IsGreaterOrEqualQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            Object innerValue = criteria.getValue().getValue();
            if (innerValue instanceof Number) {
                if (referenced.getValue() instanceof Number) {
                    Number refNumber = (Number) referenced.getValue();
                    return refNumber.doubleValue()
                            >= ((Number) innerValue).doubleValue();
                }
                return false;
            }
            if (innerValue instanceof String) {
                if (referenced.getValue() instanceof String) {
                    return ((String) referenced.getValue()).compareTo((String) innerValue)
                            >= 0;
                }
                return false;
            }
            return false;
        }

        @Override
        public Boolean visit(IsLessQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            Object innerValue = criteria.getValue().getValue();
            if (innerValue instanceof Number) {
                if (referenced.getValue() instanceof Number) {
                    Number refNumber = (Number) referenced.getValue();
                    return refNumber.doubleValue()
                            < ((Number) innerValue).doubleValue();
                }
                return false;
            }
            if (innerValue instanceof String) {
                if (referenced.getValue() instanceof String) {
                    return ((String) referenced.getValue()).compareTo((String) innerValue)
                            < 0;
                }
                return false;
            }
            return false;
        }

        @Override
        public Boolean visit(IsLessOrEqualQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            Object innerValue = criteria.getValue().getValue();
            if (innerValue instanceof Number) {
                if (referenced.getValue() instanceof Number) {
                    Number refNumber = (Number) referenced.getValue();
                    return refNumber.doubleValue()
                            <= ((Number) innerValue).doubleValue();
                }
                return false;
            }
            if (innerValue instanceof String) {
                if (referenced.getValue() instanceof String) {
                    return ((String) referenced.getValue()).compareTo((String) innerValue)
                            <= 0;
                }
                return false;
            }
            return false;
        }

        @Override
        public Boolean visit(IsObjectQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            return referenced instanceof KVDocument;
        }

        @Override
        public Boolean visit(InQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            ScalarValue<?> converted = KVValueToScalarValue.fromDocValue(referenced);
            for (ScalarValue<?> expectedValue : criteria.getValue()) {
                if (converted.equals(expectedValue)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ModIsQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (referenced.getValue() instanceof Integer
                    || referenced.getValue() instanceof Long) {
                long refValue = ((Number) referenced.getValue()).longValue();
                long reminder = refValue
                        % criteria.getDivisor().getValue().longValue();
                return reminder == criteria.getReminder().getValue().longValue();
            }
            return false;
        }

        @Override
        public Boolean visit(SizeIsQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (!(referenced instanceof KVArray)) {
                return false;
            }
            return ((KVArray) referenced).size()
                    == criteria.getValue().getValue();
        }

        @Override
        public Boolean visit(ContainsAttributesQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (!(referenced instanceof KVDocument)) {
                return false;
            }
            KVDocument refObject = (KVDocument) referenced;
            HashSet<String> keySet = Sets.newHashSet(refObject.getKeys());
            if (criteria.getAttributes().size() > keySet.size()) {
                return false;
            }
            if (criteria.isExclusive() && criteria.getAttributes().size()!= keySet.size()) {
                return false;
            }

            return keySet.containsAll(criteria.getAttributes());
        }

        @Override
        public Boolean visit(ExistsQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (!(referenced instanceof KVArray)) {
                return false;
            }
            QueryCriteria subQuery = criteria.getBody();
            for (KVValue<?> docValue : (KVArray) referenced) {
                if (subQuery.accept(this, docValue)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(MatchPatternQueryCriteria criteria, KVValue<?> arg) {
            KVValue<?> referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (!(referenced instanceof KVString)) {
                return false;
            }
            String valAsString = ((KVString) referenced).getValue();
            Matcher matcher = criteria.getPattern().matcher(valAsString);
            return matcher.matches();
        }
    }
}
