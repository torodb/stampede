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

package com.torodb.torod.mongodb.translator;

import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.*;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.*;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.querycriteria.utils.ConjunctionBuilder;
import com.torodb.torod.core.language.querycriteria.utils.DisjunctionBuilder;
import com.torodb.torod.core.language.querycriteria.utils.EqualFactory;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarArray;
import com.torodb.torod.core.subdocument.values.ScalarInteger;
import com.torodb.torod.core.utils.KVValueToScalarValue;
import com.torodb.torod.mongodb.exp.modifiers.ExpModifications;
import com.torodb.torod.mongodb.exp.modifiers.ExpModifier;
import com.torodb.torod.mongodb.exp.modifiers.RegexExpModifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

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
            BsonDocument queryObject
    ) throws UserToroException {
        return translateImplicitAnd(
                AttributeReference.EMPTY_REFERENCE,
                queryObject
        );
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
            @Nonnull BsonDocument exp
    ) throws UserToroException {
        if (exp.isEmpty()) {
            return TrueQueryCriteria.getInstance();
        }

        if (exp.size() == 1) {
            Entry<?> entry = exp.getFirstEntry();

            String key = entry.getKey();
            if (isExpModifier(key)) {
                ExpModifications newModifiers = new ExpModifications(exp);
                ClassToInstanceMap<ExpModifier> notConsumedModifiers
                        = newModifiers.getNotConsumedModifiers();
                assert !notConsumedModifiers.isEmpty();
                throw new NotConsumedExpModifierException(notConsumedModifiers);
            }
            
            BsonValue uncastedArg = exp.get(key);
            return translateExp(attRefAcum, key, uncastedArg, ExpModifications.NO_MODIFICATIONS);
        }

        ConjunctionBuilder conjunctionBuilder = new ConjunctionBuilder();

        ExpModifications newModifiers = new ExpModifications(exp);
        //TODO: Constraint merged ands, ors, nors and subqueries and equalities
        for (Entry<?> entry : exp) {
            String key = entry.getKey();
            if (isExpModifier(key)) {
                continue;
            }
            
            BsonValue uncastedArg = entry.getValue();
            conjunctionBuilder.add(
                    translateExp(
                            attRefAcum, 
                            key, 
                            uncastedArg, 
                            newModifiers
                    )
            );
        }
        ClassToInstanceMap<ExpModifier> notConsumedModifiers = newModifiers
                .getNotConsumedModifiers();
        if (!notConsumedModifiers.isEmpty()) {
            throw new NotConsumedExpModifierException(notConsumedModifiers);
        }

        return conjunctionBuilder.build();
    }

    @Nonnull
    private QueryCriteria translateExp(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull String key,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications
    ) throws UserToroException {
        switch (getExpressionType(key, uncastedArg)) {
            case AND_OR_NOR: {
                if (isAnd(key)) {
                    return translateAndOperand(attRefAcum, uncastedArg, modifications);
                }
                if (isOr(key)) {
                    return translateOrOperand(attRefAcum, uncastedArg, modifications);
                }
                if (isNor(key)) {
                    return translateNorOperand(attRefAcum, uncastedArg, modifications);
                }
                throw new ToroImplementationException("Unexpected operation");
            }
            case EQUALITY: {
                AttributeReference newAttRefAcum
                        = translateObjectAttributeReference(attRefAcum, key);
                return translateEquality(newAttRefAcum, uncastedArg, modifications);
            }
            case SUB_EXP: {
                AttributeReference newAttRefAcum
                        = translateObjectAttributeReference(attRefAcum, key);
                return translateSubQueries(newAttRefAcum, uncastedArg, modifications);
            }
            case INVALID:
            default: {
                throw new UserToroException("The query {" + key + ": "
                                                    + uncastedArg
                                                    + "} is not a valid top level expression");
            }
        }
    }

    private NodeType getExpressionType(
            @Nonnull String key,
            @Nonnull BsonValue uncastedArg
    ) {
        if (isAnd(key) || isOr(key) || isNor(key)) {
            return NodeType.AND_OR_NOR;
        }
        
        if (isExpModifier(key)) {
            throw new ToroImplementationException(
                    "An expression but not a modifier was expected"
            );
        }
        
        if (isSubQuery(key)) {
            return NodeType.INVALID;
        }

        if (!uncastedArg.isDocument()) {
            return NodeType.EQUALITY;
        }

        boolean oneSubQuery = false;
        boolean oneNoSubQuery = false;

        for (Entry<?> entry : uncastedArg.asDocument()) {
            String subKey = entry.getKey();
            if (isSubQuery(subKey)) {
                oneSubQuery |= true;
            }
            else if (!isExpModifier(subKey)) {
                oneNoSubQuery |= true;
            }
        }
        if (!oneSubQuery) {
            return NodeType.EQUALITY;
        }
        if (oneNoSubQuery) {
            return NodeType.INVALID;
        }
        return NodeType.SUB_EXP;
    }

    private boolean isAnd(@Nonnull String key   ) {
        return key.equals("$and");
    }

    private boolean isOr(@Nonnull String key) {
        return key.equals("$or");
    }

    private boolean isNor(@Nonnull String key) {
        return key.equals("$nor");
    }

    private boolean isExpModifier(@Nonnull String key) {
        return ExpModifications.isExpModifer(key);
    }
    
    private boolean isSubQuery(@Nonnull String key) {
        return QueryOperator.isSubQuery(key);
    }

    private boolean isSubQuery(@Nonnull BsonValue uncastedArg) {
        if (!uncastedArg.isDocument()) {
            return false;
        }

        BsonDocument doc = uncastedArg.asDocument();
        if (doc.isEmpty()) {
            return false;
        }
        for (Entry<?> entry : doc) {
            String key = entry.getKey();
            if (!isSubQuery(key) && !isExpModifier(key)) {
                return false;
            }
        }
        return true;
    }

    private QueryCriteria translateAndOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications
    ) throws UserToroException {
        if (!uncastedArg.isArray()) {
            throw new UserToroException(
                    "$and operand requires an array of json objects, but "
                            + uncastedArg + " is "
                            + "recived");
        }

        ConjunctionBuilder buidler = new ConjunctionBuilder();
        BsonArray argument = uncastedArg.asArray();
        for (BsonValue object : argument) {
            if (object == null || !object.isDocument()) {
                throw new UserToroException(
                        "$and operand requires an array of json objects, but "
                                + argument
                                + " contains " + object
                        + ", that is not a json object");
            }
            buidler.add(translateImplicitAnd(attRefAcum, object.asDocument()));
        }

        return buidler.build();
    }

    private QueryCriteria translateOrOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications
    ) throws UserToroException {
        if (!uncastedArg.isArray()) {
            throw new UserToroException(
                    "$or operand requires an array of json objects, but "
                            + uncastedArg + " has been "
                            + "recived");
        }

        BsonArray argument = uncastedArg.asArray();

        if (argument.isEmpty()) {
            throw new UserToroException(
                    "$or operands requires a nonempty array, but an empty one has been recived");
        }

        DisjunctionBuilder buidler = new DisjunctionBuilder();

        for (BsonValue object : argument) {
            if (object == null || !object.isDocument()) {
                throw new UserToroException(
                        "$and operand requires an array of json objects, but "
                                + argument
                                + " contains " + object
                        + ", that is not a json object");
            }
            buidler.add(translateImplicitAnd(attRefAcum, object.asDocument()));
        }

        return buidler.build();
    }

    private QueryCriteria translateNorOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications
    ) throws UserToroException {
        //http://docs.mongodb.org/manual/reference/operator/query/nor/#op._S_nor
        if (!uncastedArg.isArray()) {
            throw new UserToroException("$nor needs an array");
        }
        BsonArray value = uncastedArg.asArray();

        if (value.isEmpty()) {
            throw new UserToroException("$nor must not be a nonempty array");
        }

        return new NotQueryCriteria(
                translateOrOperand(attRefAcum, value, ExpModifications.NO_MODIFICATIONS)
        );
    }

    private QueryCriteria translateEquality(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications
    ) {
        if (uncastedArg instanceof BsonRegex) {
            BsonRegex asRegex = uncastedArg.asRegex();
            return new MatchPatternQueryCriteria(
                    attRefAcum,
                    Pattern.compile(asRegex.getPattern(), BsonRegex.Options.patternOptionsToFlags(asRegex.getOptions()))
            );
        }
        else {
            KVValue value = MongoWPConverter.translate(uncastedArg);
            return EqualFactory.createEquality(attRefAcum, value);
        }
    }

    private QueryCriteria translateSubQueries(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications
    ) throws UserToroException {
        if (!uncastedArg.isDocument()) {
            throw new ToroImplementationException("A bson object was expected");
        }
        BsonDocument arg = uncastedArg.asDocument();
        if (arg.size() == 1) {
            Entry<?> entry = arg.getFirstEntry();
            return translateSubQuery(attRefAcum, entry.getKey(), entry.getValue(), modifications);
        }
        else {
            ConjunctionBuilder cb = new ConjunctionBuilder();
            ExpModifications newModifiers = new ExpModifications(arg);
            
            for (Entry<?> entry : arg) {
                if (isExpModifier(entry.getKey())) {
                    continue;
                }
                cb.add(
                        translateSubQuery(attRefAcum, entry.getKey(), entry.getValue(), newModifiers)
                );
            }
            
            ClassToInstanceMap<ExpModifier> notConsumedModifiers = newModifiers
                    .getNotConsumedModifiers();
            if (!notConsumedModifiers.isEmpty()) {
                throw new NotConsumedExpModifierException(notConsumedModifiers);
            }
        
            return cb.build();
        }
    }

    private QueryCriteria translateSubQuery(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull String key,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications
    ) throws UserToroException {
        switch (QueryOperator.fromKey(key)) {
            case EQ_KEY:
                return translateEqOperand(attRefAcum, uncastedArg, modifications);
            case GT_KEY:
                return translateGtOperand(attRefAcum, uncastedArg, modifications);
            case GTE_KEY:
                return translateGteOperand(attRefAcum, uncastedArg, modifications);
            case LT_KEY:
                return translateLtOperand(attRefAcum, uncastedArg, modifications);
            case LTE_KEY:
                return translateLteOperand(attRefAcum, uncastedArg, modifications);
            case NE_KEY:
                return translateNeOperand(attRefAcum, uncastedArg, modifications);
            case IN_KEY:
                return translateInOperand(attRefAcum, uncastedArg, modifications);
            case NIN_KEY:
                return translateNinOperand(attRefAcum, uncastedArg, modifications);
            case NOT_KEY:
                return translateNotOperand(attRefAcum, uncastedArg, modifications);
            case EXISTS_KEY:
                return translateExistsOperand(attRefAcum, uncastedArg, modifications);
            case TYPE_KEY:
                return translateTypeOperand(attRefAcum, uncastedArg, modifications);
            case MOD_KEY:
                return translateModOperand(attRefAcum, uncastedArg, modifications);
            case REGEX_KEY:
                return translateRegexOperand(attRefAcum, uncastedArg, modifications);
            case TEXT_KEY:
                return translateTextOperand(attRefAcum, uncastedArg, modifications);
            case WHERE_KEY:
                return translateWhereOperand(attRefAcum, uncastedArg, modifications);
            case ALL_KEY:
                return translateAllOperand(attRefAcum, uncastedArg, modifications);
            case ELEM_MATCH_KEY:
                return translateElemMatchOperand(attRefAcum, uncastedArg, modifications);
            case SIZE_KEY:
                return translateSizeOperand(attRefAcum, uncastedArg, modifications);
            case GEO_WITHIN_KEY:
            case GEO_INTERSECTS_KEY:
            case NEAR_KEY:
            case NEAR_SPHERE_KEY:
            default:
                return translateUnsupportedOperation(attRefAcum, uncastedArg, modifications);
        }
    }

    private QueryCriteria translateUnsupportedOperation(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications)
            throws UserToroException {
        throw new UserToroException("The operation " + uncastedArg
                                            + " is not supported right now");
    }
    
    private QueryCriteria translateEqOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        KVValue docValue = MongoWPConverter.translate(uncastedArg);
        return new IsEqualQueryCriteria(
                attRefAcum,
                KVValueToScalarValue.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateGtOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        KVValue docValue = MongoWPConverter.translate(uncastedArg);
        return new IsGreaterQueryCriteria(
                attRefAcum,
                KVValueToScalarValue.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateGteOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        KVValue docValue = MongoWPConverter.translate(uncastedArg);
        return new IsGreaterOrEqualQueryCriteria(
                attRefAcum,
                KVValueToScalarValue.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateLtOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        KVValue docValue = MongoWPConverter.translate(uncastedArg);
        return new IsLessQueryCriteria(
                attRefAcum,
                KVValueToScalarValue.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateLteOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        KVValue docValue = MongoWPConverter.translate(uncastedArg);
        return new IsLessOrEqualQueryCriteria(
                attRefAcum,
                KVValueToScalarValue.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateNeOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        KVValue docValue = MongoWPConverter.translate(uncastedArg);
        return new NotQueryCriteria(
                EqualFactory.createEquality(
                        attRefAcum,
                        docValue
                )
        );
    }

    private QueryCriteria translateInOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        if (!uncastedArg.isArray()) {
            throw new ToroImplementationException("$in needs an array");
        }
        KVValue docValue = MongoWPConverter.translate(uncastedArg);
        return new InQueryCriteria(
                attRefAcum,
                (ScalarArray) KVValueToScalarValue.fromDocValue(docValue)
        );
    }

    private QueryCriteria translateNinOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        if (!uncastedArg.isArray()) {
            throw new ToroImplementationException("$in needs an array");
        }
        KVValue docValue = MongoWPConverter.translate(uncastedArg);
        return new NotQueryCriteria(
                new InQueryCriteria(
                        attRefAcum,
                        (ScalarArray) KVValueToScalarValue.fromDocValue(docValue)
                )
        );
    }

    private QueryCriteria translateNotOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications)
            throws UserToroException {
        if (!uncastedArg.isDocument()) {
            if (uncastedArg.isRegex()) {
                throw new ToroImplementationException(
                        "Regex are not supported right now");
            }
            throw new UserToroException(
                    "$not needs a regex (not supported right now) or document");
        }
        
        return new NotQueryCriteria(
                translateSubQueries(attRefAcum, uncastedArg, ExpModifications.NO_MODIFICATIONS)
        );
    }

    private QueryCriteria translateExistsOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications)
            throws UserToroException {
        if (!uncastedArg.isBoolean()) {
            throw new UserToroException("$exists needs a boolean");
        }
        boolean positive = uncastedArg.asBoolean().getValue();
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
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications)
            throws UserToroException {
        if (!uncastedArg.isInt32()) {
            throw new UserToroException("$type needs an integer");
        }

        Integer value = uncastedArg.asInt32().getValue();
        BsonType bsonType = BsonType.fromInt(value);
        KVType dt = MongoWPConverter.translate(bsonType);

        if (dt.equals(DocumentType.INSTANCE)) {
            return new IsObjectQueryCriteria(attRefAcum);
        }
        else {
            ScalarType bt = ScalarType.fromDocType(dt);

            return new TypeIsQueryCriteria(
                    attRefAcum,
                    bt
            );
        }

    }

    private QueryCriteria translateModOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications)
            throws UserToroException {
        if (!uncastedArg.isArray()) {
            throw new UserToroException("$mod needs an array");
        }

        BsonArray value = uncastedArg.asArray();
        if (value.size() != 2) {
            throw new UserToroException(
                    "$mod needs an array with 2 elements but " + value.size()
                            + " elements were found");
        }
        if (!(value.get(0).isNumber())) {
            throw new UserToroException("$mod needs a numeric divisor but "
                                                + value.get(0) + " was found");
        }
        if (!(value.get(1).isNumber())) {
            throw new UserToroException("$mod needs a numeric remainder but "
                                                + value.get(1) + " was found");
        }
        KVNumeric<?> divisor;
        BsonValue val0 = value.get(0);
        if (val0.isInt32()) {
            divisor = KVInteger.of(val0.asInt32().getValue());
        }
        else {
            divisor = KVLong.of(val0.asInt64().getValue());
        }

        KVNumeric<?> reminder;
        BsonValue val1 = value.get(1);
        if (val1.isInt32()) {
            reminder = KVInteger.of(val1.asInt32().getValue());
        }
        else {
            reminder = KVLong.of(val1.asInt64().getValue());
        }

        if (divisor.longValue() == 0) {
            throw new UserToroException("Divisor cannot be 0");
        }

        return new ModIsQueryCriteria(
                attRefAcum,
                KVValueToScalarValue.fromNumber(divisor),
                KVValueToScalarValue.fromNumber(reminder)
        );
    }

    private QueryCriteria translateRegexOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        String pattern;
        int flags;
        if (uncastedArg.isString()) {
            pattern = uncastedArg.asString().getValue();
            flags = 0;
        }
        else if (uncastedArg.isRegex()) {
            BsonRegex regExp = uncastedArg.asRegex();
            pattern = regExp.getPattern();
            flags = BsonRegex.Options.patternOptionsToFlags(regExp.getOptions());
        }
        else {
            throw new UserToroException("$regex has to be a string or a pattern");
        }
        
        Pattern regex;
        RegexExpModifier modifier = modifications.consumeModifier(RegexExpModifier.class);
        if (modifier != null) {
            regex = Pattern.compile(pattern, flags | modifier.toPatternFlags());
        }
        else {
            regex = Pattern.compile(pattern, flags);
        }
        return new MatchPatternQueryCriteria(attRefAcum, regex);
    }

    private QueryCriteria translateTextOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private QueryCriteria translateWhereOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private QueryCriteria translateAllOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications)
            throws UserToroException {
        if (!uncastedArg.isArray()) {
            throw new UserToroException("$all needs an array");
        }

        KVArray value = (KVArray) MongoWPConverter.translate(uncastedArg.asArray());

        if (value.isEmpty()) {
            return FalseQueryCriteria.getInstance();
        }

        QueryCriteria equality = null;
        if (value.size() == 1) { //escalar attributes are candidates
            equality = EqualFactory.createEquality(attRefAcum, value.get(0));
        }

        ConjunctionBuilder builder = new ConjunctionBuilder();
        for (KVValue child : value) {
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
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications)
            throws UserToroException {
        if (!uncastedArg.isDocument()) {
            throw new UserToroException("$elemMatch needs an object");
        }

        QueryCriteria body;
        BsonDocument castedObject = uncastedArg.asDocument();

        if (isSubQuery(uncastedArg)) {
            ExpModifications newModifications = new ExpModifications(castedObject);
            body = translateSubQueries(
                    AttributeReference.EMPTY_REFERENCE,
                    uncastedArg,
                    newModifications
            );
        }
        else {
            body = translateImplicitAnd(
                    AttributeReference.EMPTY_REFERENCE,
                    castedObject
            );
        }
        return new ExistsQueryCriteria(
                attRefAcum,
                body
        );
    }

    private QueryCriteria translateSizeOperand(
            @Nonnull AttributeReference attRefAcum,
            @Nonnull BsonValue uncastedArg,
            @Nonnull ExpModifications modifications)
            throws UserToroException {
        if (!uncastedArg.isInt32()) {
            throw new UserToroException("$size needs an integer");
        }

        return new SizeIsQueryCriteria(
                attRefAcum,
                ScalarInteger.of(uncastedArg.asInt32().intValue())
        );

    }

    private static enum NodeType {
        EQUALITY,
        SUB_EXP,
        AND_OR_NOR,
        INVALID
    }

    private static enum QueryOperator {

        EQ_KEY("$eq"),
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
