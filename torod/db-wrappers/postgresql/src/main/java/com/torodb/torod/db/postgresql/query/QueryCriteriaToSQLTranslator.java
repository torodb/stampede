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

package com.torodb.torod.db.postgresql.query;

import com.torodb.torod.core.language.querycriteria.OrQueryCriteria;
import com.torodb.torod.core.language.querycriteria.ModIsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsObjectQueryCriteria;
import com.torodb.torod.core.language.querycriteria.SizeIsQueryCriteria;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.InQueryCriteria;
import com.torodb.torod.core.language.querycriteria.FalseQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsGreaterQueryCriteria;
import com.torodb.torod.core.language.querycriteria.NotQueryCriteria;
import com.torodb.torod.core.language.querycriteria.ExistsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsGreaterOrEqualQueryCriteria;
import com.torodb.torod.core.language.querycriteria.ContainsAttributesQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsLessQueryCriteria;
import com.torodb.torod.core.language.querycriteria.AndQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsLessOrEqualQueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsEqualQueryCriteria;
import com.torodb.torod.core.language.querycriteria.TypeIsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.AttributeQueryCriteria;
import com.google.common.base.Preconditions;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaDFW;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.values.*;
import com.torodb.torod.db.postgresql.converters.ValueConverter;
import com.torodb.torod.db.postgresql.converters.jooq.SubdocValueConverter;
import com.torodb.torod.db.postgresql.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.postgresql.converters.jsonb.ValueToJsonbConverterProvider;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.jooq.*;
import org.jooq.impl.DSL;

/**
 *
 */
public class QueryCriteriaToSQLTranslator {

    private final CollectionSchema schema;
    private static final CorrectnessChecker CORRECTNESS_CHECKER
            = new CorrectnessChecker();

    public QueryCriteriaToSQLTranslator(CollectionSchema schema) {
        this.schema = schema;
    }

    /**
     * Translate a {@linkplain QueryCriteria toro query} to SQL.
     * <p>
     * This query must be able to be evaluated agains the table that represents
     * the {@link SubDocType subdocument type} of the given
     * {@linkplain DocStructure}. This is specially important in the case of
     * compound queries (like and, or, exists...). In these cases, all
     * subqueries must be able to be evaluated agains the table same table.
     * <p>
     * @param dsl
     * @param sid
     * @param subDocStructure
     * @param queryCriteria
     * @return
     */
    public Select<Record1<Integer>> translate(
            DSLContext dsl,
            int sid,
            DocStructure subDocStructure,
            QueryCriteria queryCriteria) {

        try {
            queryCriteria.accept(CORRECTNESS_CHECKER, false);
        } catch (UnexpectedQuery ex) {
            throw new UnexpectedQuery(
                    queryCriteria,
                    "sid: "+sid+". "+ex.getMessage()
            );
        }

        SubDocTable subDocTable
                = schema.getSubDocTable(subDocStructure.getType());

        Table<?> rootTable = DSL.tableByName(schema.getName(), "root");
        Field<Integer> rootSidField
                = DSL.fieldByName(Integer.class, schema.getName(), "root", "sid");
        Field<Integer> rootDidField
                = DSL.fieldByName(Integer.class, schema.getName(), "root", "did");

        int index = subDocStructure.getIndex();
        Condition indexCondition;
        if (index == 0) {
            indexCondition = subDocTable.getIndexColumn().isNull();
        }
        else {
            indexCondition = subDocTable.getIndexColumn().eq(index);
        }

        SelectConditionStep<Record1<Integer>> select
                = dsl.select(subDocTable.getDidColumn())
                .from(subDocTable)
                .join(rootTable)
                .on(subDocTable.getDidColumn().equal(rootDidField))
                .where(
                        rootSidField.equal(sid)
                ).and(indexCondition);

        final Translator inDocTranslator = new Translator();
        Condition condition = queryCriteria.accept(inDocTranslator, false);

        select.and(condition);

        return select;
    }

    private static class CorrectnessChecker extends QueryCriteriaDFW<Boolean> {

        @Override
        public Void visit(ExistsQueryCriteria criteria, Boolean inArray) {
            preExists(criteria, inArray);
            preAttributeQuery(criteria, inArray);

            criteria.getBody().accept(this, true);

            postAttributeQuery(criteria, inArray);
            postExists(criteria, inArray);

            return null;
        }

        @Override
        protected void preAttributeQuery(AttributeQueryCriteria criteria, Boolean inArray) {
            List<AttributeReference.Key> keys
                    = criteria.getAttributeReference().getKeys();

            if (inArray) {
                for (AttributeReference.Key key : keys) {
                    if (!(key instanceof AttributeReference.ArrayKey)) {
                        throw new UnexpectedQuery(criteria, "Attribute references in arrays must only contain array keys");
                    }
                }
            }
            else {
                if (keys.isEmpty()) {
                    throw new UnexpectedQuery(criteria, "A nonempty attribute reference was expected");
                }
            }
        }
    }

    private static class Translator implements
            QueryCriteriaVisitor<Condition, Boolean> {

        private String getIteratorVariableName() {
            return "value";
        }

        private String[] translateArrayRef(AttributeQueryCriteria criteria)
                throws UnexpectedQuery {
            try {
                return translateArrayRef(criteria.getAttributeReference());
            }
            catch (IllegalArgumentException ex) {
                throw new UnexpectedQuery(criteria, ex);
            }
        }

        /**
         * Given an {@linkplain AttributeReference}, returns an array of strings
         * that contains the sql keys that must be used to access the same
         * value.
         * <p>
         * If the reference must be evaluated against a column (iff it contains
         * an {@linkplain AttributeReference.ObjectKey object key}), then the
         * result is the sub reference that starts at last object key and ends
         * at the end of the given reference
         * <p>
         * If the reference must be evaluated inside an array (iff it does not
         * contain an {@linkplain AttributeReference.ArrayKey array key}), then
         * the result is the
         * {@linkplain #getIteratorVariableName() iterator variable} followed by
         * the attribute reference keys as strings
         * <p>
         * @param attRef
         * @return
         */
        private String[] translateArrayRef(AttributeReference attRef) throws
                IllegalArgumentException {
            String[] result;

            List<AttributeReference.Key> keys = attRef.getKeys();

            if (keys.isEmpty()) {
                return new String[]{getIteratorVariableName()};
            }
            int lastObjectKeyIndex = keys.size() - 1;

            while (lastObjectKeyIndex >= 0) {
                AttributeReference.Key key = keys.get(lastObjectKeyIndex);
                if (key instanceof AttributeReference.ObjectKey) {
                    break;
                }
                assert key instanceof AttributeReference.ArrayKey;
                lastObjectKeyIndex--;
            }
            assert lastObjectKeyIndex >= -1 && lastObjectKeyIndex < keys.size();

            result = new String[keys.size() - lastObjectKeyIndex];

            String firstKey;
            if (lastObjectKeyIndex < 0) { //array case
                firstKey = getIteratorVariableName();
            }
            else { //field case
                String attributeKey
                        = ((AttributeReference.ObjectKey) keys.get(lastObjectKeyIndex)).getKey();

                firstKey = SubDocTable.toColumnName(attributeKey);
            }

            result[0] = firstKey;

            for (int i = lastObjectKeyIndex + 1; i < keys.size(); i++) {
                AttributeReference.Key key = attRef.getKeys().get(i);
                if (!(key instanceof AttributeReference.ArrayKey)) {
                    throw new IllegalArgumentException(attRef.getKeys()
                            + " is not a valid attribute reference because "
                            + key + " is not an array key");
                }

                result[i + 1]
                        = Integer.toString(((AttributeReference.ArrayKey) key).getIndex());
            }
            return result;
        }

        private String getFieldName(String[] keys) {
            return getFieldName(keys, 0, keys.length);
        }

        private String getFieldName(String[] keys, int from, int to) {
            Preconditions.checkArgument(to > from, "'to' must be greater than 'from'");

            StringBuilder sb = new StringBuilder(12 + (to - from + 1) * 2);

            for (int i = from; i < to; i++) {
                sb.append(keys[i])
                        .append("->");
            }
            sb.delete(sb.length() - 2, sb.length()); //last '->' must be removed

            return sb.toString();
        }

        @Nullable
        private Condition getArrayFieldCondition(String[] keys) {
            Preconditions.checkArgument(keys.length > 0, "A non empty array was expected");
            Condition cond = null;
            final int lastArrayIndex = keys.length - 1;

            for (int i = 0; i < lastArrayIndex; i++) {
                Condition thisCond = DSL.condition("jsonb_typeof("
                        + getFieldName(keys, 0, i + 1) + ") = 'array'");
                if (cond == null) {
                    cond = thisCond;
                }
                else {
                    cond = cond.and(thisCond);
                }
            }
            return cond;
        }

        private Condition addArrayCondition(
                AttributeQueryCriteria criteria, 
                Condition criteriaCondition, 
                String[] keys, 
                boolean inArray
        ) {
            Condition arrayCondition;

            if (isInArrayValue(criteria.getAttributeReference(), inArray)) {
                arrayCondition = getArrayFieldCondition(keys);
            }
            else {
                arrayCondition = null;
            }

            if (arrayCondition != null) {
                return arrayCondition.and(criteriaCondition);
            }
            return criteriaCondition;
        }

        private String getJsonType(BasicType type) {
            switch (type) {
                case ARRAY:
                    return "array";
                case DOUBLE:
                case INTEGER:
                case LONG:
                    return "number";
                case BOOLEAN:
                    return "boolean";
                case NULL:
                    return "null";
                case STRING:
                    return "string";
                default:
                    throw new AssertionError("Unexpected basic type '" + type
                            + "'");
            }
        }

        private boolean isInArrayValue(AttributeReference attRef, boolean inArrayQuery) {
            return inArrayQuery
                    || !(attRef.getKeys().get(attRef.getKeys().size() - 1) instanceof AttributeReference.ObjectKey);
        }

        @Override
        public Condition visit(TypeIsQueryCriteria criteria, Boolean inArray) {
            AttributeReference attRef = criteria.getAttributeReference();

            if (isInArrayValue(attRef, inArray)) {
                BasicType expectedType = criteria.getExpectedType();
                switch (expectedType) {
                    case DOUBLE:
                    case INTEGER:
                    case LONG: {
                        throw new UnexpectedQuery(criteria, "It is impossible to get the specific type of a numeric array element");
                    }
                }

                String jsonType = getJsonType(expectedType);
                return DSL.condition("jsonb_typeof(?) = '?'", getFieldName(translateArrayRef(attRef)), jsonType);
            }

            throw new UnexpectedQuery(criteria, "Queries like " + criteria
                                      + " must be resolved against the structure and "
                                      + "not in the database");
        }

        @Override
        public Condition visit(ContainsAttributesQueryCriteria criteria, Boolean inArray) {
            throw new UnexpectedQuery(criteria, "Queries like " + criteria
                                      + " must be resolved against the structure and "
                                      + "not in the database");
        }

        @Override
        public Condition visit(IsObjectQueryCriteria criteria, Boolean arg) {
            throw new UnexpectedQuery(criteria, "Queries like " + criteria
                                      + " must be resolved against the structure and "
                                      + "not in the database");
        }

        @Override
        public Condition visit(TrueQueryCriteria criteria, Boolean inArray) {
            return DSL.trueCondition();
        }

        @Override
        public Condition visit(FalseQueryCriteria criteria, Boolean inArray) {
            return DSL.falseCondition();
        }

        @Override
        public Condition visit(AndQueryCriteria criteria, Boolean inArray) {
            Condition c1 = criteria.getSubQueryCriteria1().accept(this, inArray);
            Condition c2 = criteria.getSubQueryCriteria2().accept(this, inArray);

            return c1.and(c2);
        }

        @Override
        public Condition visit(OrQueryCriteria criteria, Boolean inArray) {
            Condition c1 = criteria.getSubQueryCriteria1().accept(this, inArray);
            Condition c2 = criteria.getSubQueryCriteria2().accept(this, inArray);

            return c1.or(c2);
        }

        @Override
        public Condition visit(NotQueryCriteria criteria, Boolean inArray) {
            Condition c1 = criteria.getSubQueryCriteria().accept(this, inArray);

            return c1.not();
        }

        @Override
        public Condition visit(IsEqualQueryCriteria criteria, Boolean inArray) {
            String[] keys = translateArrayRef(criteria);
            Field field = DSL.field(getFieldName(keys));
            Object value;

            Condition criteriaCondition;

            if (criteria.getValue().getType().equals(BasicType.NULL)) {
                if (!isInArrayValue(criteria.getAttributeReference(), inArray)) {
                    criteriaCondition = field.isNull();
                }
                else {
                    criteriaCondition
                            = field.equal(
                                    translateValueToJsonb(criteria.getValue())
                            );
                }
            }
            else {
                if (!isInArrayValue(criteria.getAttributeReference(), inArray)) {
                    value = translateValueToSQL(criteria.getValue());
                }
                else {
                    value = translateValueToJsonb(criteria.getValue());
                }

                criteriaCondition = field.equal(value);
            }

            return addArrayCondition(criteria, criteriaCondition, keys, inArray);
        }

        @Override
        public Condition visit(IsGreaterQueryCriteria criteria, Boolean inArray) {

            String[] keys = translateArrayRef(criteria);
            Field field = DSL.field(getFieldName(keys));
            Object value;
            Condition typeCondition = null;

            if (!isInArrayValue(criteria.getAttributeReference(), inArray)) {
                value = translateValueToSQL(criteria.getValue());
            }
            else {
                value = translateValueToJsonb(criteria.getValue());
                typeCondition = DSL.condition("jsonb_typeof(" + field + ") = '"
                        + getJsonType(criteria.getValue().getType()) + "'");
            }

            Condition criteriaCondition = field.greaterThan(value);

            if (typeCondition != null) {
                criteriaCondition = typeCondition.and(criteriaCondition);
            }

            return addArrayCondition(criteria, criteriaCondition, keys, inArray);
        }

        @Override
        public Condition visit(IsGreaterOrEqualQueryCriteria criteria, Boolean inArray) {

            String[] keys = translateArrayRef(criteria);
            Field field = DSL.field(getFieldName(keys));
            Object value;
            Condition typeCondition = null;

            if (!isInArrayValue(criteria.getAttributeReference(), inArray)) {
                value = translateValueToSQL(criteria.getValue());
            }
            else {
                value = translateValueToJsonb(criteria.getValue());
                typeCondition = DSL.condition("jsonb_typeof(" + field + ") = '"
                        + getJsonType(criteria.getValue().getType()) + "'");
            }

            Condition criteriaCondition = field.greaterOrEqual(value);

            if (typeCondition != null) {
                criteriaCondition = typeCondition.and(criteriaCondition);
            }

            return addArrayCondition(criteria, criteriaCondition, keys, inArray);
        }

        @Override
        public Condition visit(IsLessQueryCriteria criteria, Boolean inArray) {

            String[] keys = translateArrayRef(criteria);
            Field field = DSL.field(getFieldName(keys));
            Object value;
            Condition typeCondition = null;

            if (!isInArrayValue(criteria.getAttributeReference(), inArray)) {
                value = translateValueToSQL(criteria.getValue());
            }
            else {
                value = translateValueToJsonb(criteria.getValue());
                typeCondition = DSL.condition("jsonb_typeof(" + field + ") = '"
                        + getJsonType(criteria.getValue().getType()) + "'");
            }

            Condition criteriaCondition = field.lessThan(value);

            if (typeCondition != null) {
                criteriaCondition = typeCondition.and(criteriaCondition);
            }

            return addArrayCondition(criteria, criteriaCondition, keys, inArray);
        }

        @Override
        public Condition visit(IsLessOrEqualQueryCriteria criteria, Boolean inArray) {

            String[] keys = translateArrayRef(criteria);
            Field field = DSL.field(getFieldName(keys));
            Object value;
            Condition typeCondition = null;

            if (!isInArrayValue(criteria.getAttributeReference(), inArray)) {
                value = translateValueToSQL(criteria.getValue());
            }
            else {
                value = translateValueToJsonb(criteria.getValue());
                typeCondition = DSL.condition("jsonb_typeof(" + field + ") = '"
                        + getJsonType(criteria.getValue().getType()) + "'");
            }

            Condition criteriaCondition = field.lessOrEqual(value);

            if (typeCondition != null) {
                criteriaCondition = typeCondition.and(criteriaCondition);
            }

            return addArrayCondition(criteria, criteriaCondition, keys, inArray);
        }

        @Override
        public Condition visit(InQueryCriteria criteria, Boolean inArray) {
            String[] keys = translateArrayRef(criteria.getAttributeReference());

            if (!isInArrayValue(criteria.getAttributeReference(), inArray)) {

                Field field = DSL.field(getFieldName(keys));
                Object[] value = toInArgument(criteria.getValue());

                Condition criteriaCondition = field.in(value);

                return addArrayCondition(criteria, criteriaCondition, keys, inArray);

            }
            else {
                Field valueField
                        = DSL.fieldByName(String.class, getIteratorVariableName());
                Table subTable = DSL.table("jsonb_array_elements("
                        + translateValueToSQL(criteria.getValue()) + ")").as(getIteratorVariableName());

                Select<?> subQuery = DSL.select(valueField)
                        .from(subTable)
                        .where(
                                valueField.equal(
                                        DSL.field(getFieldName(keys), String.class)
                                )
                        );

                Condition criteriaCondition = DSL.exists(subQuery);
                return addArrayCondition(criteria, criteriaCondition, keys, inArray);
            }
        }

        @Override
        public Condition visit(ModIsQueryCriteria criteria, Boolean inArray) {

            String[] keys = translateArrayRef(criteria.getAttributeReference());
            Field field = DSL.field(getFieldName(keys));

            Condition criteriaCondition;
            if (!isInArrayValue(criteria.getAttributeReference(), inArray)) {
                Number divisor = criteria.getDivisor().getValue();
                Number reminder
                        = (Number) translateValueToSQL(criteria.getReminder());

                criteriaCondition = field.mod(divisor).equal(reminder);
            }
            else {
                criteriaCondition = DSL.condition(
                        field.getName() + " mod ? = ?",
                        translateValueToJsonb(criteria.getDivisor()),
                        translateValueToJsonb(criteria.getReminder())
                );

            }
            return addArrayCondition(criteria, criteriaCondition, keys, inArray);
        }

        @Override
        public Condition visit(SizeIsQueryCriteria criteria, Boolean inArray) {

            String[] keys = translateArrayRef(criteria);
            Field field = DSL.field(getFieldName(keys));
            Object value;

            boolean arraySource
                    = isInArrayValue(criteria.getAttributeReference(), inArray);

            if (!arraySource) {
                value = translateValueToSQL(criteria.getValue());
            }
            else {
                value = translateValueToJsonb(criteria.getValue());
            }

            Condition criteriaCondition = DSL.condition(
                    "jsonb_array_length(" + field.getName() + ") = ?",
                    value
            );

            if (arraySource) {
                criteriaCondition = DSL.condition("jsonb_typeof("
                        + field.getName() + ") = 'array'")
                        .and(criteriaCondition);
            }

            return addArrayCondition(criteria, criteriaCondition, keys, inArray);
        }

        @Override
        public Condition visit(ExistsQueryCriteria criteria, Boolean inArray) {
            String[] keys = translateArrayRef(criteria.getAttributeReference());
            Field field = DSL.field(getFieldName(keys));

            Field valueField = DSL.fieldByName(getIteratorVariableName());
            Table subTable 
                    = DSL.table("jsonb_array_elements(" + field.getName() + ")");

            Condition subCondition
                    = criteria.getBody().accept(this, Boolean.TRUE);

            Select<?> subQuery = DSL.select(valueField)
                    .from(subTable)
                    .where(subCondition);

            return DSL.exists(subQuery);
        }

        private Object translateValueToSQL(Value value) {
            SubdocValueConverter converter
                    = ValueToJooqConverterProvider.getConverter(value.getType());

            Object result = converter.to(value);

            if (value instanceof ArrayValue) {
                result = result.toString();
            }

            return result;
        }

        private String translateValueToJsonb(Value value) {
            ValueConverter converter
                    = ValueToJsonbConverterProvider.getInstance()
                            .getConverter(value.getType());
            
            Object translatedObject = converter.toJson(value);
            String result;

            if (value.getType().equals(BasicType.STRING)) {
                result = '\"' + translatedObject.toString() + '\"';
            }
            else {
                result = translatedObject.toString();
            }

            return result;
        }

        private Object[] toInArgument(List<Value<?>> values) {
            Object[] result = new Object[values.size()];

            for (int i = 0; i < values.size(); i++) {
                Value<? extends Serializable> value = values.get(i);
                result[i] = translateValueToSQL(value);
            }
            return result;
        }
    }
}
