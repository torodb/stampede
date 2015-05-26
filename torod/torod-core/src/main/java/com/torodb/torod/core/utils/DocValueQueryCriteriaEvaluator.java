
package com.torodb.torod.core.utils;

import com.google.common.base.Predicate;
import com.torodb.kvdocument.values.*;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.values.Value;
import java.util.Iterator;
import java.util.regex.Matcher;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class DocValueQueryCriteriaEvaluator {

    private static final ValueConverter VALUE_CONVERTER = new ValueConverter();
    
    private DocValueQueryCriteriaEvaluator()  {}
    
    public static boolean evaluate(@Nonnull QueryCriteria qc, @Nonnull DocValue value) {
        return qc.accept(
                new QueryCriteriaEvaluator(), 
                value
        );
    }
    
    public static Predicate<DocValue> createPredicate(QueryCriteria query) {
        return new QueryCriteriaPredicate(query);
    }
    
    private static class QueryCriteriaPredicate implements Predicate<DocValue> {

        private final QueryCriteria qc;
        private final QueryCriteriaEvaluator evaluator;

        public QueryCriteriaPredicate(@Nonnull QueryCriteria qc) {
            this.qc = qc;
            this.evaluator = new QueryCriteriaEvaluator();
        }
        
        @Override
        public boolean apply(DocValue value) {
            return qc.accept(evaluator, value);
        }
    }

    private static class QueryCriteriaEvaluator
            implements QueryCriteriaVisitor<Boolean, DocValue> {

        @Nullable
        private DocValue resolve(AttributeReference attRef, DocValue value) {
            if (attRef.getKeys().isEmpty()) {
                return value;
            }
            if (value instanceof ObjectValue) {
                return resolve(attRef.getKeys().iterator(), (ObjectValue) value);
            }
            if (value instanceof ArrayValue) {
                return resolve(attRef.getKeys().iterator(), (ArrayValue) value);
            }
            return null;
        }

        @Nullable
        private DocValue resolve(Iterator<AttributeReference.Key> atts, ObjectValue value) {
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
                    if (!value.contains(castedAtt.getKeyValue())) {
                        return null;
                    }
                    DocValue referencedValue
                            = value.get(castedAtt.getKeyValue());
                    if (!atts.hasNext()) {
                        return referencedValue;
                    }
                    if (referencedValue instanceof ObjectValue) {
                        return resolve(atts, (ObjectValue) referencedValue);
                    }
                    if (referencedValue instanceof ArrayValue) {
                        return resolve(atts, (ArrayValue) referencedValue);
                    }
                    else {
                        return null;
                    }
                }
            }
        }

        @Nullable
        private DocValue resolve(Iterator<AttributeReference.Key> atts, ArrayValue value) {
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
                    DocValue referencedValue
                            = value.get(castedAtt.getKeyValue());
                    if (!atts.hasNext()) {
                        return referencedValue;
                    }
                    if (referencedValue instanceof ObjectValue) {
                        return resolve(atts, (ObjectValue) referencedValue);
                    }
                    if (referencedValue instanceof ArrayValue) {
                        return resolve(atts, (ArrayValue) referencedValue);
                    }
                    else {
                        return null;
                    }
                }
            }
        }

        @Override
        public Boolean visit(TrueQueryCriteria criteria, DocValue arg) {
            return true;
        }

        @Override
        public Boolean visit(FalseQueryCriteria criteria, DocValue arg) {
            return false;
        }

        @Override
        public Boolean visit(AndQueryCriteria criteria, DocValue arg) {
            return criteria.getSubQueryCriteria1().accept(this, arg)
                    && criteria.getSubQueryCriteria2().accept(this, arg);
        }

        @Override
        public Boolean visit(OrQueryCriteria criteria, DocValue arg) {
            return criteria.getSubQueryCriteria1().accept(this, arg)
                    || criteria.getSubQueryCriteria2().accept(this, arg);
        }

        @Override
        public Boolean visit(NotQueryCriteria criteria, DocValue arg) {
            return !criteria.getSubQueryCriteria().accept(this, arg);
        }

        @Override
        public Boolean visit(TypeIsQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }

            BasicType basicType
                    = referenced.accept(VALUE_CONVERTER, null).getType();
            return criteria.getExpectedType().equals(basicType);
        }

        @Override
        public Boolean visit(IsEqualQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }

            return criteria.getValue().equals(referenced.accept(VALUE_CONVERTER, null));
        }

        @Override
        public Boolean visit(IsGreaterQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
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
        public Boolean visit(IsGreaterOrEqualQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
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
        public Boolean visit(IsLessQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
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
        public Boolean visit(IsLessOrEqualQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
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
        public Boolean visit(IsObjectQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            return referenced instanceof ObjectValue;
        }

        @Override
        public Boolean visit(InQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            Value converted = referenced.accept(VALUE_CONVERTER, null);
            for (Value<?> expectedValue : criteria.getValue()) {
                if (converted.equals(expectedValue)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ModIsQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
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
        public Boolean visit(SizeIsQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (!(referenced instanceof ArrayValue)) {
                return false;
            }
            return ((ArrayValue) referenced).size()
                    == criteria.getValue().getValue();
        }

        @Override
        public Boolean visit(ContainsAttributesQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (!(referenced instanceof ObjectValue)) {
                return false;
            }
            ObjectValue refObject = (ObjectValue) referenced;
            if (criteria.getAttributes().size() > refObject.keySet().size()) {
                return false;
            }
            if (criteria.isExclusive() && criteria.getAttributes().size()
                    != refObject.keySet().size()) {
                return false;
            }

            return refObject.keySet().containsAll(criteria.getAttributes());
        }

        @Override
        public Boolean visit(ExistsQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (!(referenced instanceof ArrayValue)) {
                return false;
            }
            QueryCriteria subQuery = criteria.getBody();
            for (DocValue docValue : (ArrayValue) referenced) {
                if (subQuery.accept(this, docValue)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(MatchPatternQueryCriteria criteria, DocValue arg) {
            DocValue referenced = resolve(criteria.getAttributeReference(), arg);
            if (referenced == null) {
                return false;
            }
            if (!(referenced instanceof StringValue)) {
                return false;
            }
            String valAsString = ((StringValue) referenced).getValue();
            Matcher matcher = criteria.getValue().getValue().matcher(valAsString);
            return matcher.matches();
        }
    }

    private static class ValueConverter implements DocValueVisitor<Value, Void> {

        @Override
        public Value visit(BooleanValue value, Void arg) {
            return com.torodb.torod.core.subdocument.values.BooleanValue.from(value.getValue());
        }

        @Override
        public Value visit(NullValue value, Void arg) {
            return com.torodb.torod.core.subdocument.values.NullValue.INSTANCE;
        }

        @Override
        public Value visit(ArrayValue value, Void arg) {
            com.torodb.torod.core.subdocument.values.ArrayValue.Builder builder
                    = new com.torodb.torod.core.subdocument.values.ArrayValue.Builder();
            for (DocValue docValue : value) {
                builder.add(docValue.accept(this, null));
            }
            return builder.build();
        }

        @Override
        public Value visit(IntegerValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.IntegerValue(value.getValue());
        }

        @Override
        public Value visit(LongValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.LongValue(value.getValue());
        }

        @Override
        public Value visit(DoubleValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.DoubleValue(value.getValue());
        }

        @Override
        public Value visit(StringValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.StringValue(value.getValue());
        }

        @Override
        public Value visit(ObjectValue value, Void arg) {
            throw new UserToroException("Object values are not supported in queries");
        }

        @Override
        public Value visit(TwelveBytesValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.TwelveBytesValue(value.getValue());
        }

        @Override
        public Value visit(DateTimeValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.DateTimeValue(value.getValue());
        }

        @Override
        public Value visit(DateValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.DateValue(value.getValue());
        }

        @Override
        public Value visit(TimeValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.TimeValue(value.getValue());
        }

        @Override
        public Value visit(PatternValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.PatternValue(value.getValue());
        }

    }
}
