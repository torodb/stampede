/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.translator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.torodb.kvdocument.conversion.mongo.MongoTypeConverter;
import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.kvdocument.types.DocType;
import com.torodb.kvdocument.types.ObjectType;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.querycriteria.utils.ConjunctionBuilder;
import com.torodb.torod.core.language.querycriteria.utils.DisjunctionBuilder;
import com.torodb.torod.core.language.querycriteria.utils.EqualFactory;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.values.ArrayValue;
import com.torodb.torod.core.subdocument.values.IntegerValue;
import com.torodb.torod.core.subdocument.values.ValueFactory;
import java.util.*;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.bson.BSONObject;

/**
 *
 */
public class BasicQueryTranslator {

    /**
     * Generates a basic torodb query from a mongodb query.
     * <p>
     * Given a mongodb query, its basic torodb query is the one that returns the
     * following subset of the original query:
     * <ol>
     * <li>All input attribute references are treated as
     * {@linkplain AttributeReference.ObjectKey object keys}. This doesn't mean
     * that all output attribute references are like object keys. Attribute
     * references generated as the body of an equality operation can contain
     * {@linkplain AttributeReferece.Arraykey array key} if the original
     * equality value contains an array that contains an object</li>
     * <li>Attribute references are strict, in other words: the semantic used by
     * mongodb that translate attribute references to something like <em>this
     * sub attribute reference points to an array that contains an object that
     * evaluates to true the same condition with the rest of the attribute
     * reference</em> is ignored</li>
     * </ol>
     * <p>
     * @param queryObject
     * @return
     * @throws UserToroException
     */
    public QueryCriteria translate(
            BSONObject queryObject
    ) throws UserToroException {
        return translateImplicitAnd(AttributeReference.EMPTY_REFERENCE,
                                    queryObject);
    }

    private AttributeReference translateObjectAttributeReference(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull String key
    ) {
        if (key.isEmpty()) {
            return attRefAcum;
        }
        ImmutableList.Builder<AttributeReference.Key> newKeysBuilder
                = ImmutableList.<AttributeReference.Key>builder().
                addAll(attRefAcum.getKeys());

        StringTokenizer st = new StringTokenizer(key, ".");
        while (st.hasMoreTokens()) {
            newKeysBuilder.add(new AttributeReference.ObjectKey(st.nextToken()));
        }

        return new AttributeReference(newKeysBuilder.build());
    }

    private QueryCriteria translateImplicitAnd(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BSONObject exp
    ) throws UserToroException {
        Set<String> keys = exp.keySet();
        if (keys.isEmpty()) {
            return TrueQueryCriteria.getInstance();
        }

        if (keys.size() == 1) {
            String key = keys.iterator().
                    next();
            Object uncastedArg = exp.get(key);
            return translateExp(attRefAcum, key, uncastedArg);
        }

        ConjunctionBuilder conjunctionBuilder = new ConjunctionBuilder();

        //TODO: Constraint merged ands, ors, nors and subqueries and equalities
        for (String key : keys) {
            Object uncastedArg = exp.get(key);
            conjunctionBuilder.add(translateExp(attRefAcum, key, uncastedArg));
        }

        return conjunctionBuilder.build();
    }

    private QueryCriteria translateExp(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull String key,
            @Nonnull Object uncastedArg
    ) throws UserToroException {
        switch (getExpressionType(key, uncastedArg)) {
            case AND_OR_NOR: {
                if (isAnd(key)) {
                    return translateAndOperand(attRefAcum, uncastedArg);
                }
                if (isOr(key)) {
                    return translateOrOperand(attRefAcum, uncastedArg);
                }
                if (isNor(key)) {
                    return translateNorOperand(attRefAcum, uncastedArg);
                }
                throw new ToroImplementationException("Unexpected operation");
            }
            case EQUALITY: {
                AttributeReference newAttRefAcum
                        = translateObjectAttributeReference(attRefAcum, key);
                return translateEquality(newAttRefAcum, uncastedArg);
            }
            case SUB_EXP: {
                AttributeReference newAttRefAcum
                        = translateObjectAttributeReference(attRefAcum, key);
                return translateSubQueries(newAttRefAcum, uncastedArg);
            }
            case INVALID:
            default: {
                throw new UserToroException("The query {" + key + ": "
                                                    + uncastedArg
                                                    + "} is not a valid top level expression");
            }
        }
    }

    private ExpressionType getExpressionType(
            @Nonnull String key,
            @Nonnull Object uncastedArg
    ) {
        if (isAnd(key) || isOr(key) || isNor(key)) {
            return ExpressionType.AND_OR_NOR;
        }
        
        if (isSubQuery(key)) {
            return ExpressionType.INVALID;
        }

        if (!(uncastedArg instanceof BSONObject)) {
            return ExpressionType.EQUALITY;
        }

        Set<String> keySet = ((BSONObject) uncastedArg).keySet();

        boolean oneSubQuery = false;
        boolean oneNoSubQuery = false;

        for (String subKey : keySet) {
            if (isSubQuery(subKey)) {
                oneSubQuery |= true;
            }
            else {
                oneNoSubQuery |= true;
            }
        }
        if (!oneSubQuery) {
            return ExpressionType.EQUALITY;
        }
        if (oneNoSubQuery) {
            return ExpressionType.INVALID;
        }
        return ExpressionType.SUB_EXP;
    }

    private boolean isAnd(
            @Nonnull String key
    ) {
        return key.equals("$and");
    }

    private boolean isOr(
            @Nonnull String key
    ) {
        return key.equals("$or");
    }

    private boolean isNor(
            @Nonnull String key) {
        return key.equals("$nor");
    }

    private boolean isSubQuery(@Nonnull String key) {
        return QueryOperator.isSubQuery(key);
    }

    private boolean isSubQuery(@Nonnull Object uncastedArg) {
        if (!(uncastedArg instanceof BSONObject)) {
            return false;
        }

        Set<String> keySet = ((BSONObject) uncastedArg).keySet();
        if (keySet.isEmpty()) {
            return false;
        }
        for (String key : keySet) {
            if (!isSubQuery(key)) {
                return false;
            }
        }
        return true;
    }

    private QueryCriteria translateAndOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg
    ) throws UserToroException {
        if (!(uncastedArg instanceof List)) {
            throw new UserToroException(
                    "$and operand requires an array of json objects, but "
                            + uncastedArg + " is "
                            + "recived");
        }

        ConjunctionBuilder buidler = new ConjunctionBuilder();
        List argument = (List) uncastedArg;
        for (Object object : argument) {
            if (object == null || !(object instanceof BSONObject)) {
                throw new UserToroException(
                        "$and operand requires an array of json objects, but "
                                + argument
                                + " contains " + object
                        + ", that is not a json object");
            }
            buidler.add(translateImplicitAnd(attRefAcum, (BSONObject) object));
        }

        return buidler.build();
    }

    private QueryCriteria translateOrOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg
    ) throws UserToroException {
        if (!(uncastedArg instanceof List)) {
            throw new UserToroException(
                    "$or operand requires an array of json objects, but "
                            + uncastedArg + " has been "
                            + "recived");
        }

        List argument = (List) uncastedArg;

        if (argument.isEmpty()) {
            throw new UserToroException(
                    "$or operands requires a nonempty array, but an empty one has been recived");
        }

        DisjunctionBuilder buidler = new DisjunctionBuilder();

        for (Object object : argument) {
            if (object == null || !(object instanceof BSONObject)) {
                throw new UserToroException(
                        "$and operand requires an array of json objects, but "
                                + argument
                                + " contains " + object
                        + ", that is not a json object");
            }
            buidler.add(translateImplicitAnd(attRefAcum, (BSONObject) object));
        }

        return buidler.build();
    }

    private QueryCriteria translateNorOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg
    ) throws UserToroException {
        //http://docs.mongodb.org/manual/reference/operator/query/nor/#op._S_nor
        if (!(uncastedArg instanceof List)) {
            throw new UserToroException("$nor needs an array");
        }
        List value = (List) uncastedArg;

        if (value.isEmpty()) {
            throw new UserToroException("$nor must not be a nonempty array");
        }

        return new NotQueryCriteria(
                translateOrOperand(attRefAcum, value)
        );
    }

    private QueryCriteria translateEquality(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg
    ) {
        DocValue value = MongoValueConverter.translateBSON(uncastedArg);

        return EqualFactory.createEquality(attRefAcum, value);
    }

    private QueryCriteria translateSubQueries(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg
    ) throws UserToroException {
        if (!(uncastedArg instanceof BSONObject)) {
            throw new ToroImplementationException("A bson object was expected");
        }
        BSONObject arg = (BSONObject) uncastedArg;
        if (arg.keySet().size() == 1) {
            String key = arg.keySet().
                    iterator().
                    next();
            return translateSubQuery(attRefAcum, key, arg.get(key));
        }
        else {
            ConjunctionBuilder cb = new ConjunctionBuilder();
            for (String key : arg.keySet()) {
                cb.add(
                        translateSubQuery(attRefAcum, key, arg.get(key))
                );
            }
            return cb.build();
        }
    }

    private QueryCriteria translateSubQuery(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull String key,
            @Nonnull Object uncastedArg
    ) throws UserToroException {
        switch (QueryOperator.fromKey(key)) {
            case GT_KEY:
                return translateGtOperand(attRefAcum, uncastedArg);
            case GTE_KEY:
                return translateGteOperand(attRefAcum, uncastedArg);
            case LT_KEY:
                return translateLtOperand(attRefAcum, uncastedArg);
            case LTE_KEY:
                return translateLteOperand(attRefAcum, uncastedArg);
            case NE_KEY:
                return translateNeOperand(attRefAcum, uncastedArg);
            case IN_KEY:
                return translateInOperand(attRefAcum, uncastedArg);
            case NIN_KEY:
                return translateNinOperand(attRefAcum, uncastedArg);
            case NOT_KEY:
                return translateNotOperand(attRefAcum, uncastedArg);
            case EXISTS_KEY:
                return translateExistsOperand(attRefAcum, uncastedArg);
            case TYPE_KEY:
                return translateTypeOperand(attRefAcum, uncastedArg);
            case MOD_KEY:
                return translateModOperand(attRefAcum, uncastedArg);
            case REGEX_KEY:
                return translateRegexOperand(attRefAcum, uncastedArg);
            case TEXT_KEY:
                return translateTextOperand(attRefAcum, uncastedArg);
            case WHERE_KEY:
                return translateWhereOperand(attRefAcum, uncastedArg);
            case ALL_KEY:
                return translateAllOperand(attRefAcum, uncastedArg);
            case ELEM_MATCH_KEY:
                return translateElemMatchOperand(attRefAcum, uncastedArg);
            case SIZE_KEY:
                return translateSizeOperand(attRefAcum, uncastedArg);
            case GEO_WITHIN_KEY:
            case GEO_INTERSECTS_KEY:
            case NEAR_KEY:
            case NEAR_SPHERE_KEY:
            default:
                return translateUnsupportedOperation(attRefAcum, uncastedArg);
        }
    }

    private QueryCriteria translateUnsupportedOperation(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg)
            throws UserToroException {
        throw new UserToroException("The operation " + uncastedArg
                                            + " is not supported right now");
    }

    private QueryCriteria translateGtOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        DocValue docValue = MongoValueConverter.translateBSON(uncastedArg);
        return new IsGreaterQueryCriteria(
                attRefAcum,
                ValueFactory.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateGteOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        DocValue docValue = MongoValueConverter.translateBSON(uncastedArg);
        return new IsGreaterOrEqualQueryCriteria(
                attRefAcum,
                ValueFactory.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateLtOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        DocValue docValue = MongoValueConverter.translateBSON(uncastedArg);
        return new IsLessQueryCriteria(
                attRefAcum,
                ValueFactory.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateLteOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        DocValue docValue = MongoValueConverter.translateBSON(uncastedArg);
        return new IsLessOrEqualQueryCriteria(
                attRefAcum,
                ValueFactory.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateNeOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        DocValue docValue = MongoValueConverter.translateBSON(uncastedArg);
        return new NotQueryCriteria(
                EqualFactory.createEquality(
                        attRefAcum,
                        docValue
                )
        );
    }

    private QueryCriteria translateInOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        if (!(uncastedArg instanceof List)) {
            throw new ToroImplementationException("$in needs an array");
        }
        DocValue docValue = MongoValueConverter.translateBSON(uncastedArg);
        return new InQueryCriteria(
                attRefAcum,
                (ArrayValue) ValueFactory.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateNinOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        if (!(uncastedArg instanceof List)) {
            throw new ToroImplementationException("$in needs an array");
        }
        DocValue docValue = MongoValueConverter.translateBSON(uncastedArg);
        return new NotQueryCriteria(
                new InQueryCriteria(
                        attRefAcum,
                        (ArrayValue) ValueFactory.fromDocValue(docValue)
                )
        );
    }

    private QueryCriteria translateNotOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg)
            throws UserToroException {
        if (!(uncastedArg instanceof BSONObject)) {
            if (uncastedArg instanceof Pattern) {
                throw new ToroImplementationException(
                        "Regex are not supported right now");
            }
            throw new UserToroException(
                    "$not needs a regex (not supported right now) or document");
        }

        return new NotQueryCriteria(
                translateSubQueries(attRefAcum, uncastedArg)
        );
    }

    private QueryCriteria translateExistsOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg)
            throws UserToroException {
        if (!(uncastedArg instanceof Boolean)) {
            throw new UserToroException("$exists needs a boolean");
        }
        Boolean positive = (Boolean) uncastedArg;
        List<AttributeReference.Key> keys = attRefAcum.getKeys();

        AttributeReference target;
        switch (keys.size()) {
            case 0: {
                throw new UserToroException(
                        "$exists needs a not-empty attribute reference");
            }
            case 1: {
                target = AttributeReference.EMPTY_REFERENCE;
                break;
            }
            default: {
                target = attRefAcum.subReference(0, keys.size() - 1);
                break;
            }
        }
        assert !keys.isEmpty();
        AttributeReference.Key lastKey = keys.get(keys.size() - 1);
        if (!(lastKey instanceof AttributeReference.ObjectKey)) {
            throw new UserToroException(
                    "$exists needs an object key as last attribute reference key");
        }
        String lastKeyName = ((AttributeReference.ObjectKey) lastKey).getKey();

        QueryCriteria result = new ContainsAttributesQueryCriteria(
                target,
                Collections.singleton(lastKeyName),
                false
        );

        if (!positive) {
            return new NotQueryCriteria(result);
        }
        return result;
    }

    private QueryCriteria translateTypeOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg)
            throws UserToroException {
        if (!(uncastedArg instanceof Integer)) {
            throw new UserToroException("$type needs an integer");
        }

        Integer value = (Integer) uncastedArg;
        DocType dt = MongoTypeConverter.translateType(value);

        if (dt.equals(ObjectType.INSTANCE)) {
            return new IsObjectQueryCriteria(attRefAcum);
        }
        else {
            BasicType bt = BasicType.fromDocType(dt);

            return new TypeIsQueryCriteria(
                    attRefAcum,
                    bt
            );
        }

    }

    private QueryCriteria translateModOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg)
            throws UserToroException {
        if (!(uncastedArg instanceof List)) {
            throw new UserToroException("$mod needs an array");
        }

        List value = (List) uncastedArg;
        if (value.size() != 2) {
            throw new UserToroException(
                    "$mod needs an array with 2 elements but " + value.size()
                            + " elements were found");
        }
        if (!(value.get(0) instanceof Number)) {
            throw new UserToroException("$mod needs a numeric divisor but "
                                                + value.get(0) + " was found");
        }
        if (!(value.get(1) instanceof Number)) {
            throw new UserToroException("$mod needs a numeric remainder but "
                                                + value.get(1) + " was found");
        }
        Number divisor = (Number) value.get(0);
        Number reminder = (Number) value.get(1);

        if (divisor.equals(0)) {
            throw new UserToroException("Divisor cannot be 0");
        }

        return new ModIsQueryCriteria(
                attRefAcum,
                ValueFactory.fromNumber(divisor),
                ValueFactory.fromNumber(reminder)
        );
    }

    private QueryCriteria translateRegexOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private QueryCriteria translateTextOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private QueryCriteria translateWhereOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private QueryCriteria translateAllOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg)
            throws UserToroException {
        if (!(uncastedArg instanceof List)) {
            throw new UserToroException("$all needs an array");
        }

        com.torodb.kvdocument.values.ArrayValue value = MongoValueConverter.
                translateArray((List) uncastedArg);

        if (value.isEmpty()) {
            return FalseQueryCriteria.getInstance();
        }

        QueryCriteria equality = null;
        if (value.size() == 1) { //escalar attributes are candidates
            equality = EqualFactory.createEquality(attRefAcum, value.get(0));
        }

        ConjunctionBuilder builder = new ConjunctionBuilder();
        for (DocValue child : value) {
            builder.add(
                    new ExistsQueryCriteria(
                            attRefAcum,
                            EqualFactory.createEquality(
                                    AttributeReference.EMPTY_REFERENCE, child))
            );
        }

        if (equality != null) {
            return new OrQueryCriteria(equality, builder.build());
        }
        else {
            return builder.build();
        }
    }

    private QueryCriteria translateElemMatchOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg)
            throws UserToroException {
        if (!(uncastedArg instanceof BSONObject)) {
            throw new UserToroException("$elemMatch needs an object");
        }

        QueryCriteria body;
        BSONObject castedObject = (BSONObject) uncastedArg;

        if (isSubQuery(uncastedArg)) {
            body = translateSubQueries(AttributeReference.EMPTY_REFERENCE,
                                       uncastedArg);
        }
        else {
            body = translateImplicitAnd(AttributeReference.EMPTY_REFERENCE,
                                        castedObject);
        }
        return new ExistsQueryCriteria(
                attRefAcum,
                body
        );
    }

    private QueryCriteria translateSizeOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull Object uncastedArg)
            throws UserToroException {
        Object uncastedValue = uncastedArg;
        if (!(uncastedValue instanceof Integer)) {
            throw new UserToroException("$size needs an integer");
        }

        return new SizeIsQueryCriteria(
                attRefAcum,
                (IntegerValue) ValueFactory.fromNumber((Number) uncastedValue)
        );

    }

    private static enum ExpressionType {

        EQUALITY,
        SUB_EXP,
        AND_OR_NOR,
        INVALID
    }

    private static enum QueryOperator {

        GT_KEY("$gt"),
        GTE_KEY("$gte"),
        LT_KEY("$lt"),
        LTE_KEY("$lte"),
        NE_KEY("$ne"),
        IN_KEY("$in"),
        NIN_KEY("$nin"),
        NOT_KEY("$not"),
        EXISTS_KEY("$exists"),
        TYPE_KEY("$type"),
        MOD_KEY("$mod"),
        REGEX_KEY("$regex"),
        TEXT_KEY("$text"),
        WHERE_KEY("$where"),
        GEO_WITHIN_KEY("$geoWithin"),
        GEO_INTERSECTS_KEY("$getIntersects"),
        NEAR_KEY("$near"),
        NEAR_SPHERE_KEY("$nearSphere"),
        ALL_KEY("$all"),
        ELEM_MATCH_KEY("$elemMatch"),
        SIZE_KEY("$size");

        private static final Map<String, QueryOperator> operandsByKey;
        private final String key;

        static {
            operandsByKey = Maps.newHashMapWithExpectedSize(QueryOperator.
                    values().length);
            for (QueryOperator operand : QueryOperator.values()) {
                operandsByKey.put(operand.key, operand);
            }
        }

        private QueryOperator(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        public static boolean isSubQuery(String key) {
            return operandsByKey.containsKey(key);
        }

        @Nonnull
        public static QueryOperator fromKey(String key) {
            QueryOperator result = operandsByKey.get(key);
            if (result == null) {
                throw new IllegalArgumentException(
                        "There is no operand whose key is '" + key + "'");
            }
            return result;
        }
    }
}
