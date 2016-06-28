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

package com.torodb.backend;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Map;

import org.jooq.BetweenAndStep;
import org.jooq.Binding;
import org.jooq.Comparator;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.QuantifiedSelect;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.SortField;
import org.jooq.SortOrder;
import org.jooq.WindowIgnoreNullsStep;
import org.jooq.WindowPartitionByStep;

import com.torodb.core.d2r.DocPartRow;

@SuppressWarnings("unchecked")
public abstract class InternalField<T> implements Field<T> {
    
    private static final long serialVersionUID = 1L;
    
    private final Field<T> field;

    public static class DidInternalField extends InternalField<Integer> {
        
        private static final long serialVersionUID = 1L;
        
        public DidInternalField(Field<Integer> field) {
            super(field);
        }
        
        @Override
        public boolean isDid() {
            return true;
        }

        @Override
        public void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws SQLException {
            preparedStatement.setInt(index, docPartRow.getDid());
        }
        
        public Integer getValue(DocPartRow docPartRow){
        	return docPartRow.getDid();
        }
    }

    public static class RidInternalField extends InternalField<Integer> {
        
        private static final long serialVersionUID = 1L;
        
        public RidInternalField(Field<Integer> field) {
            super(field);
        }
        
        @Override
        public boolean isRid() {
            return true;
        }

        @Override
        public void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws SQLException {
            preparedStatement.setInt(index, docPartRow.getRid());
        }
        
        public Integer getValue(DocPartRow docPartRow){
        	return docPartRow.getRid();
        }

    }

    public static class PidInternalField extends InternalField<Integer> {
        
        private static final long serialVersionUID = 1L;
        
        public PidInternalField(Field<Integer> field) {
            super(field);
        }
        
        @Override
        public boolean isPid() {
            return true;
        }

        @Override
        public void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws SQLException {
            preparedStatement.setInt(index, docPartRow.getPid());
        }
        
        public Integer getValue(DocPartRow docPartRow){
        	return docPartRow.getPid();
        }

    }

    public static class SeqInternalField extends InternalField<Integer> {
        
        private static final long serialVersionUID = 1L;
        
        public SeqInternalField(Field<Integer> field) {
            super(field);
        }
        
        @Override
        public boolean isSeq() {
            return true;
        }

        @Override
        public void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws SQLException {
            if (docPartRow.getSeq() != null) {
                preparedStatement.setInt(index, docPartRow.getSeq());
            } else {
                preparedStatement.setNull(index, Types.INTEGER);
            }
        }
        
        public Integer getValue(DocPartRow docPartRow){
        	return docPartRow.getSeq();
        }

    }
    
    public InternalField(Field<T> field) {
        super();
        this.field = field;
    }

    public abstract void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws SQLException;
    
    public abstract T getValue(DocPartRow docPartRow);
    
    public boolean isDid() {
        return false;
    }
    
    public boolean isRid() {
        return false;
    }
    
    public boolean isPid() {
        return false;
    }
    
    public boolean isSeq() {
        return false;
    }
    
    public String toString() {
        return field.toString();
    }

    public String getName() {
        return field.getName();
    }

    public String getComment() {
        return field.getComment();
    }

    public int hashCode() {
        return field.hashCode();
    }

    public Converter<?, T> getConverter() {
        return field.getConverter();
    }

    public Binding<?, T> getBinding() {
        return field.getBinding();
    }

    public Class<T> getType() {
        return field.getType();
    }

    public DataType<T> getDataType() {
        return field.getDataType();
    }

    public DataType<T> getDataType(Configuration configuration) {
        return field.getDataType(configuration);
    }

    public Field<T> as(String alias) {
        return field.as(alias);
    }

    public Field<T> as(Field<?> otherField) {
        return field.as(otherField);
    }

    public boolean equals(Object other) {
        return field.equals(other);
    }

    public <Z> Field<Z> cast(Field<Z> field) {
        return field.cast(field);
    }

    public <Z> Field<Z> cast(DataType<Z> type) {
        return field.cast(type);
    }

    public <Z> Field<Z> cast(Class<Z> type) {
        return field.cast(type);
    }

    public <Z> Field<Z> coerce(Field<Z> field) {
        return field.coerce(field);
    }

    public <Z> Field<Z> coerce(DataType<Z> type) {
        return field.coerce(type);
    }

    public <Z> Field<Z> coerce(Class<Z> type) {
        return field.coerce(type);
    }

    public SortField<T> asc() {
        return field.asc();
    }

    public SortField<T> desc() {
        return field.desc();
    }

    public SortField<T> sort(SortOrder order) {
        return field.sort(order);
    }

    public SortField<Integer> sortAsc(Collection<T> sortList) {
        return field.sortAsc(sortList);
    }

    public SortField<Integer> sortAsc(T... sortList) {
        return field.sortAsc(sortList);
    }

    public SortField<Integer> sortDesc(Collection<T> sortList) {
        return field.sortDesc(sortList);
    }

    public SortField<Integer> sortDesc(T... sortList) {
        return field.sortDesc(sortList);
    }

    public <Z> SortField<Z> sort(Map<T, Z> sortMap) {
        return field.sort(sortMap);
    }

    public Field<T> neg() {
        return field.neg();
    }

    public Field<T> add(Number value) {
        return field.add(value);
    }

    public Field<T> add(Field<?> value) {
        return field.add(value);
    }

    public Field<T> plus(Number value) {
        return field.plus(value);
    }

    public Field<T> plus(Field<?> value) {
        return field.plus(value);
    }

    public Field<T> sub(Number value) {
        return field.sub(value);
    }

    public Field<T> sub(Field<?> value) {
        return field.sub(value);
    }

    public Field<T> subtract(Number value) {
        return field.subtract(value);
    }

    public Field<T> subtract(Field<?> value) {
        return field.subtract(value);
    }

    public Field<T> minus(Number value) {
        return field.minus(value);
    }

    public Field<T> minus(Field<?> value) {
        return field.minus(value);
    }

    public Field<T> mul(Number value) {
        return field.mul(value);
    }

    public Field<T> mul(Field<? extends Number> value) {
        return field.mul(value);
    }

    public Field<T> multiply(Number value) {
        return field.multiply(value);
    }

    public Field<T> multiply(Field<? extends Number> value) {
        return field.multiply(value);
    }

    public Field<T> div(Number value) {
        return field.div(value);
    }

    public Field<T> div(Field<? extends Number> value) {
        return field.div(value);
    }

    public Field<T> divide(Number value) {
        return field.divide(value);
    }

    public Field<T> divide(Field<? extends Number> value) {
        return field.divide(value);
    }

    public Field<T> mod(Number value) {
        return field.mod(value);
    }

    public Field<T> mod(Field<? extends Number> value) {
        return field.mod(value);
    }

    public Field<T> modulo(Number value) {
        return field.modulo(value);
    }

    public Field<T> modulo(Field<? extends Number> value) {
        return field.modulo(value);
    }

    public Field<T> bitNot() {
        return field.bitNot();
    }

    public Field<T> bitAnd(T value) {
        return field.bitAnd(value);
    }

    public Field<T> bitAnd(Field<T> value) {
        return field.bitAnd(value);
    }

    public Field<T> bitNand(T value) {
        return field.bitNand(value);
    }

    public Field<T> bitNand(Field<T> value) {
        return field.bitNand(value);
    }

    public Field<T> bitOr(T value) {
        return field.bitOr(value);
    }

    public Field<T> bitOr(Field<T> value) {
        return field.bitOr(value);
    }

    public Field<T> bitNor(T value) {
        return field.bitNor(value);
    }

    public Field<T> bitNor(Field<T> value) {
        return field.bitNor(value);
    }

    public Field<T> bitXor(T value) {
        return field.bitXor(value);
    }

    public Field<T> bitXor(Field<T> value) {
        return field.bitXor(value);
    }

    public Field<T> bitXNor(T value) {
        return field.bitXNor(value);
    }

    public Field<T> bitXNor(Field<T> value) {
        return field.bitXNor(value);
    }

    public Field<T> shl(Number value) {
        return field.shl(value);
    }

    public Field<T> shl(Field<? extends Number> value) {
        return field.shl(value);
    }

    public Field<T> shr(Number value) {
        return field.shr(value);
    }

    public Field<T> shr(Field<? extends Number> value) {
        return field.shr(value);
    }

    public Condition isNull() {
        return field.isNull();
    }

    public Condition isNotNull() {
        return field.isNotNull();
    }

    public Condition isDistinctFrom(T value) {
        return field.isDistinctFrom(value);
    }

    public Condition isDistinctFrom(Field<T> field) {
        return field.isDistinctFrom(field);
    }

    public Condition isNotDistinctFrom(T value) {
        return field.isNotDistinctFrom(value);
    }

    public Condition isNotDistinctFrom(Field<T> field) {
        return field.isNotDistinctFrom(field);
    }

    public Condition likeRegex(String pattern) {
        return field.likeRegex(pattern);
    }

    public Condition likeRegex(Field<String> pattern) {
        return field.likeRegex(pattern);
    }

    public Condition notLikeRegex(String pattern) {
        return field.notLikeRegex(pattern);
    }

    public Condition notLikeRegex(Field<String> pattern) {
        return field.notLikeRegex(pattern);
    }

    public Condition like(Field<String> value) {
        return field.like(value);
    }

    public Condition like(Field<String> value, char escape) {
        return field.like(value, escape);
    }

    public Condition like(String value) {
        return field.like(value);
    }

    public Condition like(String value, char escape) {
        return field.like(value, escape);
    }

    public Condition likeIgnoreCase(Field<String> field) {
        return field.likeIgnoreCase(field);
    }

    public Condition likeIgnoreCase(Field<String> field, char escape) {
        return field.likeIgnoreCase(field, escape);
    }

    public Condition likeIgnoreCase(String value) {
        return field.likeIgnoreCase(value);
    }

    public Condition likeIgnoreCase(String value, char escape) {
        return field.likeIgnoreCase(value, escape);
    }

    public Condition notLike(Field<String> field) {
        return field.notLike(field);
    }

    public Condition notLike(Field<String> field, char escape) {
        return field.notLike(field, escape);
    }

    public Condition notLike(String value) {
        return field.notLike(value);
    }

    public Condition notLike(String value, char escape) {
        return field.notLike(value, escape);
    }

    public Condition notLikeIgnoreCase(Field<String> field) {
        return field.notLikeIgnoreCase(field);
    }

    public Condition notLikeIgnoreCase(Field<String> field, char escape) {
        return field.notLikeIgnoreCase(field, escape);
    }

    public Condition notLikeIgnoreCase(String value) {
        return field.notLikeIgnoreCase(value);
    }

    public Condition notLikeIgnoreCase(String value, char escape) {
        return field.notLikeIgnoreCase(value, escape);
    }

    public Condition contains(T value) {
        return field.contains(value);
    }

    public Condition contains(Field<T> value) {
        return field.contains(value);
    }

    public Condition startsWith(T value) {
        return field.startsWith(value);
    }

    public Condition startsWith(Field<T> value) {
        return field.startsWith(value);
    }

    public Condition endsWith(T value) {
        return field.endsWith(value);
    }

    public Condition endsWith(Field<T> value) {
        return field.endsWith(value);
    }

    public Condition in(Collection<?> values) {
        return field.in(values);
    }

    public Condition in(Result<? extends Record1<T>> result) {
        return field.in(result);
    }

    public Condition in(T... values) {
        return field.in(values);
    }

    public Condition in(Field<?>... values) {
        return field.in(values);
    }

    public Condition in(Select<? extends Record1<T>> query) {
        return field.in(query);
    }

    public Condition notIn(Collection<?> values) {
        return field.notIn(values);
    }

    public Condition notIn(Result<? extends Record1<T>> result) {
        return field.notIn(result);
    }

    public Condition notIn(T... values) {
        return field.notIn(values);
    }

    public Condition notIn(Field<?>... values) {
        return field.notIn(values);
    }

    public Condition notIn(Select<? extends Record1<T>> query) {
        return field.notIn(query);
    }

    public Condition between(T minValue, T maxValue) {
        return field.between(minValue, maxValue);
    }

    public Condition between(Field<T> minValue, Field<T> maxValue) {
        return field.between(minValue, maxValue);
    }

    public Condition betweenSymmetric(T minValue, T maxValue) {
        return field.betweenSymmetric(minValue, maxValue);
    }

    public Condition betweenSymmetric(Field<T> minValue, Field<T> maxValue) {
        return field.betweenSymmetric(minValue, maxValue);
    }

    public Condition notBetween(T minValue, T maxValue) {
        return field.notBetween(minValue, maxValue);
    }

    public Condition notBetween(Field<T> minValue, Field<T> maxValue) {
        return field.notBetween(minValue, maxValue);
    }

    public Condition notBetweenSymmetric(T minValue, T maxValue) {
        return field.notBetweenSymmetric(minValue, maxValue);
    }

    public Condition notBetweenSymmetric(Field<T> minValue, Field<T> maxValue) {
        return field.notBetweenSymmetric(minValue, maxValue);
    }

    public BetweenAndStep<T> between(T minValue) {
        return field.between(minValue);
    }

    public BetweenAndStep<T> between(Field<T> minValue) {
        return field.between(minValue);
    }

    public BetweenAndStep<T> betweenSymmetric(T minValue) {
        return field.betweenSymmetric(minValue);
    }

    public BetweenAndStep<T> betweenSymmetric(Field<T> minValue) {
        return field.betweenSymmetric(minValue);
    }

    public BetweenAndStep<T> notBetween(T minValue) {
        return field.notBetween(minValue);
    }

    public BetweenAndStep<T> notBetween(Field<T> minValue) {
        return field.notBetween(minValue);
    }

    public BetweenAndStep<T> notBetweenSymmetric(T minValue) {
        return field.notBetweenSymmetric(minValue);
    }

    public BetweenAndStep<T> notBetweenSymmetric(Field<T> minValue) {
        return field.notBetweenSymmetric(minValue);
    }

    public Condition compare(Comparator comparator, T value) {
        return field.compare(comparator, value);
    }

    public Condition compare(Comparator comparator, Field<T> field) {
        return field.compare(comparator, field);
    }

    public Condition compare(Comparator comparator, Select<? extends Record1<T>> query) {
        return field.compare(comparator, query);
    }

    public Condition compare(Comparator comparator, QuantifiedSelect<? extends Record1<T>> query) {
        return field.compare(comparator, query);
    }

    public Condition equal(T value) {
        return field.equal(value);
    }

    public Condition equal(Field<T> field) {
        return field.equal(field);
    }

    public Condition equal(Select<? extends Record1<T>> query) {
        return field.equal(query);
    }

    public Condition equal(QuantifiedSelect<? extends Record1<T>> query) {
        return field.equal(query);
    }

    public Condition eq(T value) {
        return field.eq(value);
    }

    public Condition eq(Field<T> field) {
        return field.eq(field);
    }

    public Condition eq(Select<? extends Record1<T>> query) {
        return field.eq(query);
    }

    public Condition eq(QuantifiedSelect<? extends Record1<T>> query) {
        return field.eq(query);
    }

    public Condition notEqual(T value) {
        return field.notEqual(value);
    }

    public Condition notEqual(Field<T> field) {
        return field.notEqual(field);
    }

    public Condition notEqual(Select<? extends Record1<T>> query) {
        return field.notEqual(query);
    }

    public Condition notEqual(QuantifiedSelect<? extends Record1<T>> query) {
        return field.notEqual(query);
    }

    public Condition ne(T value) {
        return field.ne(value);
    }

    public Condition ne(Field<T> field) {
        return field.ne(field);
    }

    public Condition ne(Select<? extends Record1<T>> query) {
        return field.ne(query);
    }

    public Condition ne(QuantifiedSelect<? extends Record1<T>> query) {
        return field.ne(query);
    }

    public Condition lessThan(T value) {
        return field.lessThan(value);
    }

    public Condition lessThan(Field<T> field) {
        return field.lessThan(field);
    }

    public Condition lessThan(Select<? extends Record1<T>> query) {
        return field.lessThan(query);
    }

    public Condition lessThan(QuantifiedSelect<? extends Record1<T>> query) {
        return field.lessThan(query);
    }

    public Condition lt(T value) {
        return field.lt(value);
    }

    public Condition lt(Field<T> field) {
        return field.lt(field);
    }

    public Condition lt(Select<? extends Record1<T>> query) {
        return field.lt(query);
    }

    public Condition lt(QuantifiedSelect<? extends Record1<T>> query) {
        return field.lt(query);
    }

    public Condition lessOrEqual(T value) {
        return field.lessOrEqual(value);
    }

    public Condition lessOrEqual(Field<T> field) {
        return field.lessOrEqual(field);
    }

    public Condition lessOrEqual(Select<? extends Record1<T>> query) {
        return field.lessOrEqual(query);
    }

    public Condition lessOrEqual(QuantifiedSelect<? extends Record1<T>> query) {
        return field.lessOrEqual(query);
    }

    public Condition le(T value) {
        return field.le(value);
    }

    public Condition le(Field<T> field) {
        return field.le(field);
    }

    public Condition le(Select<? extends Record1<T>> query) {
        return field.le(query);
    }

    public Condition le(QuantifiedSelect<? extends Record1<T>> query) {
        return field.le(query);
    }

    public Condition greaterThan(T value) {
        return field.greaterThan(value);
    }

    public Condition greaterThan(Field<T> field) {
        return field.greaterThan(field);
    }

    public Condition greaterThan(Select<? extends Record1<T>> query) {
        return field.greaterThan(query);
    }

    public Condition greaterThan(QuantifiedSelect<? extends Record1<T>> query) {
        return field.greaterThan(query);
    }

    public Condition gt(T value) {
        return field.gt(value);
    }

    public Condition gt(Field<T> field) {
        return field.gt(field);
    }

    public Condition gt(Select<? extends Record1<T>> query) {
        return field.gt(query);
    }

    public Condition gt(QuantifiedSelect<? extends Record1<T>> query) {
        return field.gt(query);
    }

    public Condition greaterOrEqual(T value) {
        return field.greaterOrEqual(value);
    }

    public Condition greaterOrEqual(Field<T> field) {
        return field.greaterOrEqual(field);
    }

    public Condition greaterOrEqual(Select<? extends Record1<T>> query) {
        return field.greaterOrEqual(query);
    }

    public Condition greaterOrEqual(QuantifiedSelect<? extends Record1<T>> query) {
        return field.greaterOrEqual(query);
    }

    public Condition ge(T value) {
        return field.ge(value);
    }

    public Condition ge(Field<T> field) {
        return field.ge(field);
    }

    public Condition ge(Select<? extends Record1<T>> query) {
        return field.ge(query);
    }

    public Condition ge(QuantifiedSelect<? extends Record1<T>> query) {
        return field.ge(query);
    }

    public Condition isTrue() {
        return field.isTrue();
    }

    public Condition isFalse() {
        return field.isFalse();
    }

    public Condition equalIgnoreCase(String value) {
        return field.equalIgnoreCase(value);
    }

    public Condition equalIgnoreCase(Field<String> value) {
        return field.equalIgnoreCase(value);
    }

    public Condition notEqualIgnoreCase(String value) {
        return field.notEqualIgnoreCase(value);
    }

    public Condition notEqualIgnoreCase(Field<String> value) {
        return field.notEqualIgnoreCase(value);
    }

    public Field<Integer> sign() {
        return field.sign();
    }

    public Field<T> abs() {
        return field.abs();
    }

    public Field<T> round() {
        return field.round();
    }

    public Field<T> round(int decimals) {
        return field.round(decimals);
    }

    public Field<T> floor() {
        return field.floor();
    }

    public Field<T> ceil() {
        return field.ceil();
    }

    public Field<BigDecimal> sqrt() {
        return field.sqrt();
    }

    public Field<BigDecimal> exp() {
        return field.exp();
    }

    public Field<BigDecimal> ln() {
        return field.ln();
    }

    public Field<BigDecimal> log(int base) {
        return field.log(base);
    }

    public Field<BigDecimal> pow(Number exponent) {
        return field.pow(exponent);
    }

    public Field<BigDecimal> power(Number exponent) {
        return field.power(exponent);
    }

    public Field<BigDecimal> acos() {
        return field.acos();
    }

    public Field<BigDecimal> asin() {
        return field.asin();
    }

    public Field<BigDecimal> atan() {
        return field.atan();
    }

    public Field<BigDecimal> atan2(Number y) {
        return field.atan2(y);
    }

    public Field<BigDecimal> atan2(Field<? extends Number> y) {
        return field.atan2(y);
    }

    public Field<BigDecimal> cos() {
        return field.cos();
    }

    public Field<BigDecimal> sin() {
        return field.sin();
    }

    public Field<BigDecimal> tan() {
        return field.tan();
    }

    public Field<BigDecimal> cot() {
        return field.cot();
    }

    public Field<BigDecimal> sinh() {
        return field.sinh();
    }

    public Field<BigDecimal> cosh() {
        return field.cosh();
    }

    public Field<BigDecimal> tanh() {
        return field.tanh();
    }

    public Field<BigDecimal> coth() {
        return field.coth();
    }

    public Field<BigDecimal> deg() {
        return field.deg();
    }

    public Field<BigDecimal> rad() {
        return field.rad();
    }

    public Field<Integer> count() {
        return field.count();
    }

    public Field<Integer> countDistinct() {
        return field.countDistinct();
    }

    public Field<T> max() {
        return field.max();
    }

    public Field<T> min() {
        return field.min();
    }

    public Field<BigDecimal> sum() {
        return field.sum();
    }

    public Field<BigDecimal> avg() {
        return field.avg();
    }

    public Field<BigDecimal> median() {
        return field.median();
    }

    public Field<BigDecimal> stddevPop() {
        return field.stddevPop();
    }

    public Field<BigDecimal> stddevSamp() {
        return field.stddevSamp();
    }

    public Field<BigDecimal> varPop() {
        return field.varPop();
    }

    public Field<BigDecimal> varSamp() {
        return field.varSamp();
    }

    public WindowPartitionByStep<Integer> countOver() {
        return field.countOver();
    }

    public WindowPartitionByStep<T> maxOver() {
        return field.maxOver();
    }

    public WindowPartitionByStep<T> minOver() {
        return field.minOver();
    }

    public WindowPartitionByStep<BigDecimal> sumOver() {
        return field.sumOver();
    }

    public WindowPartitionByStep<BigDecimal> avgOver() {
        return field.avgOver();
    }

    public WindowIgnoreNullsStep<T> firstValue() {
        return field.firstValue();
    }

    public WindowIgnoreNullsStep<T> lastValue() {
        return field.lastValue();
    }

    public WindowIgnoreNullsStep<T> lead() {
        return field.lead();
    }

    public WindowIgnoreNullsStep<T> lead(int offset) {
        return field.lead(offset);
    }

    public WindowIgnoreNullsStep<T> lead(int offset, T defaultValue) {
        return field.lead(offset, defaultValue);
    }

    public WindowIgnoreNullsStep<T> lead(int offset, Field<T> defaultValue) {
        return field.lead(offset, defaultValue);
    }

    public WindowIgnoreNullsStep<T> lag() {
        return field.lag();
    }

    public WindowIgnoreNullsStep<T> lag(int offset) {
        return field.lag(offset);
    }

    public WindowIgnoreNullsStep<T> lag(int offset, T defaultValue) {
        return field.lag(offset, defaultValue);
    }

    public WindowIgnoreNullsStep<T> lag(int offset, Field<T> defaultValue) {
        return field.lag(offset, defaultValue);
    }

    public WindowPartitionByStep<BigDecimal> stddevPopOver() {
        return field.stddevPopOver();
    }

    public WindowPartitionByStep<BigDecimal> stddevSampOver() {
        return field.stddevSampOver();
    }

    public WindowPartitionByStep<BigDecimal> varPopOver() {
        return field.varPopOver();
    }

    public WindowPartitionByStep<BigDecimal> varSampOver() {
        return field.varSampOver();
    }

    public Field<String> upper() {
        return field.upper();
    }

    public Field<String> lower() {
        return field.lower();
    }

    public Field<String> trim() {
        return field.trim();
    }

    public Field<String> rtrim() {
        return field.rtrim();
    }

    public Field<String> ltrim() {
        return field.ltrim();
    }

    public Field<String> rpad(Field<? extends Number> length) {
        return field.rpad(length);
    }

    public Field<String> rpad(int length) {
        return field.rpad(length);
    }

    public Field<String> rpad(Field<? extends Number> length, Field<String> character) {
        return field.rpad(length, character);
    }

    public Field<String> rpad(int length, char character) {
        return field.rpad(length, character);
    }

    public Field<String> lpad(Field<? extends Number> length) {
        return field.lpad(length);
    }

    public Field<String> lpad(int length) {
        return field.lpad(length);
    }

    public Field<String> lpad(Field<? extends Number> length, Field<String> character) {
        return field.lpad(length, character);
    }

    public Field<String> lpad(int length, char character) {
        return field.lpad(length, character);
    }

    public Field<String> repeat(Number count) {
        return field.repeat(count);
    }

    public Field<String> repeat(Field<? extends Number> count) {
        return field.repeat(count);
    }

    public Field<String> replace(Field<String> search) {
        return field.replace(search);
    }

    public Field<String> replace(String search) {
        return field.replace(search);
    }

    public Field<String> replace(Field<String> search, Field<String> replace) {
        return field.replace(search, replace);
    }

    public Field<String> replace(String search, String replace) {
        return field.replace(search, replace);
    }

    public Field<Integer> position(String search) {
        return field.position(search);
    }

    public Field<Integer> position(Field<String> search) {
        return field.position(search);
    }

    public Field<Integer> ascii() {
        return field.ascii();
    }

    public Field<String> concat(Field<?>... fields) {
        return field.concat(fields);
    }

    public Field<String> concat(String... values) {
        return field.concat(values);
    }

    public Field<String> substring(int startingPosition) {
        return field.substring(startingPosition);
    }

    public Field<String> substring(Field<? extends Number> startingPosition) {
        return field.substring(startingPosition);
    }

    public Field<String> substring(int startingPosition, int length) {
        return field.substring(startingPosition, length);
    }

    public Field<String> substring(Field<? extends Number> startingPosition, Field<? extends Number> length) {
        return field.substring(startingPosition, length);
    }

    public Field<Integer> length() {
        return field.length();
    }

    public Field<Integer> charLength() {
        return field.charLength();
    }

    public Field<Integer> bitLength() {
        return field.bitLength();
    }

    public Field<Integer> octetLength() {
        return field.octetLength();
    }

    public Field<Integer> extract(DatePart datePart) {
        return field.extract(datePart);
    }

    public Field<T> greatest(T... others) {
        return field.greatest(others);
    }

    public Field<T> greatest(Field<?>... others) {
        return field.greatest(others);
    }

    public Field<T> least(T... others) {
        return field.least(others);
    }

    public Field<T> least(Field<?>... others) {
        return field.least(others);
    }

    public Field<T> nvl(T defaultValue) {
        return field.nvl(defaultValue);
    }

    public Field<T> nvl(Field<T> defaultValue) {
        return field.nvl(defaultValue);
    }

    public <Z> Field<Z> nvl2(Z valueIfNotNull, Z valueIfNull) {
        return field.nvl2(valueIfNotNull, valueIfNull);
    }

    public <Z> Field<Z> nvl2(Field<Z> valueIfNotNull, Field<Z> valueIfNull) {
        return field.nvl2(valueIfNotNull, valueIfNull);
    }

    public Field<T> nullif(T other) {
        return field.nullif(other);
    }

    public Field<T> nullif(Field<T> other) {
        return field.nullif(other);
    }

    public <Z> Field<Z> decode(T search, Z result) {
        return field.decode(search, result);
    }

    public <Z> Field<Z> decode(T search, Z result, Object... more) {
        return field.decode(search, result, more);
    }

    public <Z> Field<Z> decode(Field<T> search, Field<Z> result) {
        return field.decode(search, result);
    }

    public <Z> Field<Z> decode(Field<T> search, Field<Z> result, Field<?>... more) {
        return field.decode(search, result, more);
    }

    public Field<T> coalesce(T option, T... options) {
        return field.coalesce(option, options);
    }

    public Field<T> coalesce(Field<T> option, Field<?>... options) {
        return field.coalesce(option, options);
    }
}
