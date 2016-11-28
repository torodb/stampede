/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend;


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

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public class DelegatorField<T> implements Field<T> {

  private static final long serialVersionUID = 4060506762956191613L;

  private final Field<T> delegate;

  public DelegatorField(Field<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getComment() {
    return delegate.getComment();
  }

  @Override
  public Converter<?, T> getConverter() {
    return delegate.getConverter();
  }

  @Override
  public Binding<?, T> getBinding() {
    return delegate.getBinding();
  }

  @Override
  public Class<T> getType() {
    return delegate.getType();
  }

  @Override
  public DataType<T> getDataType() {
    return delegate.getDataType();
  }

  @Override
  public DataType<T> getDataType(Configuration configuration) {
    return delegate.getDataType(configuration);
  }

  @Override
  public Field<T> as(String alias) {
    return delegate.as(alias);
  }

  @Override
  public Field<T> as(Field<?> otherField) {
    return delegate.as(otherField);
  }

  @Override
  public boolean equals(Object other) {
    return delegate.equals(other);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public <Z> Field<Z> cast(Field<Z> field) {
    return delegate.cast(field);
  }

  @Override
  public <Z> Field<Z> cast(DataType<Z> type) {
    return delegate.cast(type);
  }

  @Override
  public <Z> Field<Z> cast(Class<Z> type) {
    return delegate.cast(type);
  }

  @Override
  public <Z> Field<Z> coerce(Field<Z> field) {
    return delegate.coerce(field);
  }

  @Override
  public <Z> Field<Z> coerce(DataType<Z> type) {
    return delegate.coerce(type);
  }

  @Override
  public <Z> Field<Z> coerce(Class<Z> type) {
    return delegate.coerce(type);
  }

  @Override
  public SortField<T> asc() {
    return delegate.asc();
  }

  @Override
  public SortField<T> desc() {
    return delegate.desc();
  }

  @Override
  public SortField<T> sort(SortOrder order) {
    return delegate.sort(order);
  }

  @Override
  public SortField<Integer> sortAsc(Collection<T> sortList) {
    return delegate.sortAsc(sortList);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SortField<Integer> sortAsc(T... sortList) {
    return delegate.sortAsc(sortList);
  }

  @Override
  public SortField<Integer> sortDesc(Collection<T> sortList) {
    return delegate.sortDesc(sortList);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SortField<Integer> sortDesc(T... sortList) {
    return delegate.sortDesc(sortList);
  }

  @Override
  public <Z> SortField<Z> sort(Map<T, Z> sortMap) {
    return delegate.sort(sortMap);
  }

  @Override
  public Field<T> neg() {
    return delegate.neg();
  }

  @Override
  public Field<T> add(Number value) {
    return delegate.add(value);
  }

  @Override
  public Field<T> add(Field<?> value) {
    return delegate.add(value);
  }

  @Override
  public Field<T> plus(Number value) {
    return delegate.plus(value);
  }

  @Override
  public Field<T> plus(Field<?> value) {
    return delegate.plus(value);
  }

  @Override
  public Field<T> sub(Number value) {
    return delegate.sub(value);
  }

  @Override
  public Field<T> sub(Field<?> value) {
    return delegate.sub(value);
  }

  @Override
  public Field<T> subtract(Number value) {
    return delegate.subtract(value);
  }

  @Override
  public Field<T> subtract(Field<?> value) {
    return delegate.subtract(value);
  }

  @Override
  public Field<T> minus(Number value) {
    return delegate.minus(value);
  }

  @Override
  public Field<T> minus(Field<?> value) {
    return delegate.minus(value);
  }

  @Override
  public Field<T> mul(Number value) {
    return delegate.mul(value);
  }

  @Override
  public Field<T> mul(Field<? extends Number> value) {
    return delegate.mul(value);
  }

  @Override
  public Field<T> multiply(Number value) {
    return delegate.multiply(value);
  }

  @Override
  public Field<T> multiply(Field<? extends Number> value) {
    return delegate.multiply(value);
  }

  @Override
  public Field<T> div(Number value) {
    return delegate.div(value);
  }

  @Override
  public Field<T> div(Field<? extends Number> value) {
    return delegate.div(value);
  }

  @Override
  public Field<T> divide(Number value) {
    return delegate.divide(value);
  }

  @Override
  public Field<T> divide(Field<? extends Number> value) {
    return delegate.divide(value);
  }

  @Override
  public Field<T> mod(Number value) {
    return delegate.mod(value);
  }

  @Override
  public Field<T> mod(Field<? extends Number> value) {
    return delegate.mod(value);
  }

  @Override
  public Field<T> modulo(Number value) {
    return delegate.modulo(value);
  }

  @Override
  public Field<T> modulo(Field<? extends Number> value) {
    return delegate.modulo(value);
  }

  @Override
  public Field<T> bitNot() {
    return delegate.bitNot();
  }

  @Override
  public Field<T> bitAnd(T value) {
    return delegate.bitAnd(value);
  }

  @Override
  public Field<T> bitAnd(Field<T> value) {
    return delegate.bitAnd(value);
  }

  @Override
  public Field<T> bitNand(T value) {
    return delegate.bitNand(value);
  }

  @Override
  public Field<T> bitNand(Field<T> value) {
    return delegate.bitNand(value);
  }

  @Override
  public Field<T> bitOr(T value) {
    return delegate.bitOr(value);
  }

  @Override
  public Field<T> bitOr(Field<T> value) {
    return delegate.bitOr(value);
  }

  @Override
  public Field<T> bitNor(T value) {
    return delegate.bitNor(value);
  }

  @Override
  public Field<T> bitNor(Field<T> value) {
    return delegate.bitNor(value);
  }

  @Override
  public Field<T> bitXor(T value) {
    return delegate.bitXor(value);
  }

  @Override
  public Field<T> bitXor(Field<T> value) {
    return delegate.bitXor(value);
  }

  @Override
  public Field<T> bitXNor(T value) {
    return delegate.bitXNor(value);
  }

  @Override
  public Field<T> bitXNor(Field<T> value) {
    return delegate.bitXNor(value);
  }

  @Override
  public Field<T> shl(Number value) {
    return delegate.shl(value);
  }

  @Override
  public Field<T> shl(Field<? extends Number> value) {
    return delegate.shl(value);
  }

  @Override
  public Field<T> shr(Number value) {
    return delegate.shr(value);
  }

  @Override
  public Field<T> shr(Field<? extends Number> value) {
    return delegate.shr(value);
  }

  @Override
  public Condition isNull() {
    return delegate.isNull();
  }

  @Override
  public Condition isNotNull() {
    return delegate.isNotNull();
  }

  @Override
  public Condition isDistinctFrom(T value) {
    return delegate.isDistinctFrom(value);
  }

  @Override
  public Condition isDistinctFrom(Field<T> field) {
    return delegate.isDistinctFrom(field);
  }

  @Override
  public Condition isNotDistinctFrom(T value) {
    return delegate.isNotDistinctFrom(value);
  }

  @Override
  public Condition isNotDistinctFrom(Field<T> field) {
    return delegate.isNotDistinctFrom(field);
  }

  @Override
  public Condition likeRegex(String pattern) {
    return delegate.likeRegex(pattern);
  }

  @Override
  public Condition likeRegex(Field<String> pattern) {
    return delegate.likeRegex(pattern);
  }

  @Override
  public Condition notLikeRegex(String pattern) {
    return delegate.notLikeRegex(pattern);
  }

  @Override
  public Condition notLikeRegex(Field<String> pattern) {
    return delegate.notLikeRegex(pattern);
  }

  @Override
  public Condition like(Field<String> value) {
    return delegate.like(value);
  }

  @Override
  public Condition like(Field<String> value, char escape) {
    return delegate.like(value, escape);
  }

  @Override
  public Condition like(String value) {
    return delegate.like(value);
  }

  @Override
  public Condition like(String value, char escape) {
    return delegate.like(value, escape);
  }

  @Override
  public Condition likeIgnoreCase(Field<String> field) {
    return delegate.likeIgnoreCase(field);
  }

  @Override
  public Condition likeIgnoreCase(Field<String> field, char escape) {
    return delegate.likeIgnoreCase(field, escape);
  }

  @Override
  public Condition likeIgnoreCase(String value) {
    return delegate.likeIgnoreCase(value);
  }

  @Override
  public Condition likeIgnoreCase(String value, char escape) {
    return delegate.likeIgnoreCase(value, escape);
  }

  @Override
  public Condition notLike(Field<String> field) {
    return delegate.notLike(field);
  }

  @Override
  public Condition notLike(Field<String> field, char escape) {
    return delegate.notLike(field, escape);
  }

  @Override
  public Condition notLike(String value) {
    return delegate.notLike(value);
  }

  @Override
  public Condition notLike(String value, char escape) {
    return delegate.notLike(value, escape);
  }

  @Override
  public Condition notLikeIgnoreCase(Field<String> field) {
    return delegate.notLikeIgnoreCase(field);
  }

  @Override
  public Condition notLikeIgnoreCase(Field<String> field, char escape) {
    return delegate.notLikeIgnoreCase(field, escape);
  }

  @Override
  public Condition notLikeIgnoreCase(String value) {
    return delegate.notLikeIgnoreCase(value);
  }

  @Override
  public Condition notLikeIgnoreCase(String value, char escape) {
    return delegate.notLikeIgnoreCase(value, escape);
  }

  @Override
  public Condition contains(T value) {
    return delegate.contains(value);
  }

  @Override
  public Condition contains(Field<T> value) {
    return delegate.contains(value);
  }

  @Override
  public Condition startsWith(T value) {
    return delegate.startsWith(value);
  }

  @Override
  public Condition startsWith(Field<T> value) {
    return delegate.startsWith(value);
  }

  @Override
  public Condition endsWith(T value) {
    return delegate.endsWith(value);
  }

  @Override
  public Condition endsWith(Field<T> value) {
    return delegate.endsWith(value);
  }

  @Override
  public Condition in(Collection<?> values) {
    return delegate.in(values);
  }

  @Override
  public Condition in(Result<? extends Record1<T>> result) {
    return delegate.in(result);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Condition in(T... values) {
    return delegate.in(values);
  }

  @Override
  public Condition in(Field<?>... values) {
    return delegate.in(values);
  }

  @Override
  public Condition in(Select<? extends Record1<T>> query) {
    return delegate.in(query);
  }

  @Override
  public Condition notIn(Collection<?> values) {
    return delegate.notIn(values);
  }

  @Override
  public Condition notIn(Result<? extends Record1<T>> result) {
    return delegate.notIn(result);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Condition notIn(T... values) {
    return delegate.notIn(values);
  }

  @Override
  public Condition notIn(Field<?>... values) {
    return delegate.notIn(values);
  }

  @Override
  public Condition notIn(Select<? extends Record1<T>> query) {
    return delegate.notIn(query);
  }

  @Override
  public Condition between(T minValue, T maxValue) {
    return delegate.between(minValue, maxValue);
  }

  @Override
  public Condition between(Field<T> minValue, Field<T> maxValue) {
    return delegate.between(minValue, maxValue);
  }

  @Override
  public Condition betweenSymmetric(T minValue, T maxValue) {
    return delegate.betweenSymmetric(minValue, maxValue);
  }

  @Override
  public Condition betweenSymmetric(Field<T> minValue, Field<T> maxValue) {
    return delegate.betweenSymmetric(minValue, maxValue);
  }

  @Override
  public Condition notBetween(T minValue, T maxValue) {
    return delegate.notBetween(minValue, maxValue);
  }

  @Override
  public Condition notBetween(Field<T> minValue, Field<T> maxValue) {
    return delegate.notBetween(minValue, maxValue);
  }

  @Override
  public Condition notBetweenSymmetric(T minValue, T maxValue) {
    return delegate.notBetweenSymmetric(minValue, maxValue);
  }

  @Override
  public Condition notBetweenSymmetric(Field<T> minValue, Field<T> maxValue) {
    return delegate.notBetweenSymmetric(minValue, maxValue);
  }

  @Override
  public BetweenAndStep<T> between(T minValue) {
    return delegate.between(minValue);
  }

  @Override
  public BetweenAndStep<T> between(Field<T> minValue) {
    return delegate.between(minValue);
  }

  @Override
  public BetweenAndStep<T> betweenSymmetric(T minValue) {
    return delegate.betweenSymmetric(minValue);
  }

  @Override
  public BetweenAndStep<T> betweenSymmetric(Field<T> minValue) {
    return delegate.betweenSymmetric(minValue);
  }

  @Override
  public BetweenAndStep<T> notBetween(T minValue) {
    return delegate.notBetween(minValue);
  }

  @Override
  public BetweenAndStep<T> notBetween(Field<T> minValue) {
    return delegate.notBetween(minValue);
  }

  @Override
  public BetweenAndStep<T> notBetweenSymmetric(T minValue) {
    return delegate.notBetweenSymmetric(minValue);
  }

  @Override
  public BetweenAndStep<T> notBetweenSymmetric(Field<T> minValue) {
    return delegate.notBetweenSymmetric(minValue);
  }

  @Override
  public Condition compare(Comparator comparator, T value) {
    return delegate.compare(comparator, value);
  }

  @Override
  public Condition compare(Comparator comparator, Field<T> field) {
    return delegate.compare(comparator, field);
  }

  @Override
  public Condition compare(Comparator comparator, Select<? extends Record1<T>> query) {
    return delegate.compare(comparator, query);
  }

  @Override
  public Condition compare(Comparator comparator, QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.compare(comparator, query);
  }

  @Override
  public Condition equal(T value) {
    return delegate.equal(value);
  }

  @Override
  public Condition equal(Field<T> field) {
    return delegate.equal(field);
  }

  @Override
  public Condition equal(Select<? extends Record1<T>> query) {
    return delegate.equal(query);
  }

  @Override
  public Condition equal(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.equal(query);
  }

  @Override
  public Condition eq(T value) {
    return delegate.eq(value);
  }

  @Override
  public Condition eq(Field<T> field) {
    return delegate.eq(field);
  }

  @Override
  public Condition eq(Select<? extends Record1<T>> query) {
    return delegate.eq(query);
  }

  @Override
  public Condition eq(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.eq(query);
  }

  @Override
  public Condition notEqual(T value) {
    return delegate.notEqual(value);
  }

  @Override
  public Condition notEqual(Field<T> field) {
    return delegate.notEqual(field);
  }

  @Override
  public Condition notEqual(Select<? extends Record1<T>> query) {
    return delegate.notEqual(query);
  }

  @Override
  public Condition notEqual(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.notEqual(query);
  }

  @Override
  public Condition ne(T value) {
    return delegate.ne(value);
  }

  @Override
  public Condition ne(Field<T> field) {
    return delegate.ne(field);
  }

  @Override
  public Condition ne(Select<? extends Record1<T>> query) {
    return delegate.ne(query);
  }

  @Override
  public Condition ne(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.ne(query);
  }

  @Override
  public Condition lessThan(T value) {
    return delegate.lessThan(value);
  }

  @Override
  public Condition lessThan(Field<T> field) {
    return delegate.lessThan(field);
  }

  @Override
  public Condition lessThan(Select<? extends Record1<T>> query) {
    return delegate.lessThan(query);
  }

  @Override
  public Condition lessThan(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.lessThan(query);
  }

  @Override
  public Condition lt(T value) {
    return delegate.lt(value);
  }

  @Override
  public Condition lt(Field<T> field) {
    return delegate.lt(field);
  }

  @Override
  public Condition lt(Select<? extends Record1<T>> query) {
    return delegate.lt(query);
  }

  @Override
  public Condition lt(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.lt(query);
  }

  @Override
  public Condition lessOrEqual(T value) {
    return delegate.lessOrEqual(value);
  }

  @Override
  public Condition lessOrEqual(Field<T> field) {
    return delegate.lessOrEqual(field);
  }

  @Override
  public Condition lessOrEqual(Select<? extends Record1<T>> query) {
    return delegate.lessOrEqual(query);
  }

  @Override
  public Condition lessOrEqual(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.lessOrEqual(query);
  }

  @Override
  public Condition le(T value) {
    return delegate.le(value);
  }

  @Override
  public Condition le(Field<T> field) {
    return delegate.le(field);
  }

  @Override
  public Condition le(Select<? extends Record1<T>> query) {
    return delegate.le(query);
  }

  @Override
  public Condition le(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.le(query);
  }

  @Override
  public Condition greaterThan(T value) {
    return delegate.greaterThan(value);
  }

  @Override
  public Condition greaterThan(Field<T> field) {
    return delegate.greaterThan(field);
  }

  @Override
  public Condition greaterThan(Select<? extends Record1<T>> query) {
    return delegate.greaterThan(query);
  }

  @Override
  public Condition greaterThan(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.greaterThan(query);
  }

  @Override
  public Condition gt(T value) {
    return delegate.gt(value);
  }

  @Override
  public Condition gt(Field<T> field) {
    return delegate.gt(field);
  }

  @Override
  public Condition gt(Select<? extends Record1<T>> query) {
    return delegate.gt(query);
  }

  @Override
  public Condition gt(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.gt(query);
  }

  @Override
  public Condition greaterOrEqual(T value) {
    return delegate.greaterOrEqual(value);
  }

  @Override
  public Condition greaterOrEqual(Field<T> field) {
    return delegate.greaterOrEqual(field);
  }

  @Override
  public Condition greaterOrEqual(Select<? extends Record1<T>> query) {
    return delegate.greaterOrEqual(query);
  }

  @Override
  public Condition greaterOrEqual(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.greaterOrEqual(query);
  }

  @Override
  public Condition ge(T value) {
    return delegate.ge(value);
  }

  @Override
  public Condition ge(Field<T> field) {
    return delegate.ge(field);
  }

  @Override
  public Condition ge(Select<? extends Record1<T>> query) {
    return delegate.ge(query);
  }

  @Override
  public Condition ge(QuantifiedSelect<? extends Record1<T>> query) {
    return delegate.ge(query);
  }

  @Override
  public Condition isTrue() {
    return delegate.isTrue();
  }

  @Override
  public Condition isFalse() {
    return delegate.isFalse();
  }

  @Override
  public Condition equalIgnoreCase(String value) {
    return delegate.equalIgnoreCase(value);
  }

  @Override
  public Condition equalIgnoreCase(Field<String> value) {
    return delegate.equalIgnoreCase(value);
  }

  @Override
  public Condition notEqualIgnoreCase(String value) {
    return delegate.notEqualIgnoreCase(value);
  }

  @Override
  public Condition notEqualIgnoreCase(Field<String> value) {
    return delegate.notEqualIgnoreCase(value);
  }

  @Override
  public Field<Integer> sign() {
    return delegate.sign();
  }

  @Override
  public Field<T> abs() {
    return delegate.abs();
  }

  @Override
  public Field<T> round() {
    return delegate.round();
  }

  @Override
  public Field<T> round(int decimals) {
    return delegate.round(decimals);
  }

  @Override
  public Field<T> floor() {
    return delegate.floor();
  }

  @Override
  public Field<T> ceil() {
    return delegate.ceil();
  }

  @Override
  public Field<BigDecimal> sqrt() {
    return delegate.sqrt();
  }

  @Override
  public Field<BigDecimal> exp() {
    return delegate.exp();
  }

  @Override
  public Field<BigDecimal> ln() {
    return delegate.ln();
  }

  @Override
  public Field<BigDecimal> log(int base) {
    return delegate.log(base);
  }

  @Override
  public Field<BigDecimal> pow(Number exponent) {
    return delegate.pow(exponent);
  }

  @Override
  public Field<BigDecimal> power(Number exponent) {
    return delegate.power(exponent);
  }

  @Override
  public Field<BigDecimal> acos() {
    return delegate.acos();
  }

  @Override
  public Field<BigDecimal> asin() {
    return delegate.asin();
  }

  @Override
  public Field<BigDecimal> atan() {
    return delegate.atan();
  }

  @Override
  public Field<BigDecimal> atan2(Number y) {
    return delegate.atan2(y);
  }

  @Override
  public Field<BigDecimal> atan2(Field<? extends Number> y) {
    return delegate.atan2(y);
  }

  @Override
  public Field<BigDecimal> cos() {
    return delegate.cos();
  }

  @Override
  public Field<BigDecimal> sin() {
    return delegate.sin();
  }

  @Override
  public Field<BigDecimal> tan() {
    return delegate.tan();
  }

  @Override
  public Field<BigDecimal> cot() {
    return delegate.cot();
  }

  @Override
  public Field<BigDecimal> sinh() {
    return delegate.sinh();
  }

  @Override
  public Field<BigDecimal> cosh() {
    return delegate.cosh();
  }

  @Override
  public Field<BigDecimal> tanh() {
    return delegate.tanh();
  }

  @Override
  public Field<BigDecimal> coth() {
    return delegate.coth();
  }

  @Override
  public Field<BigDecimal> deg() {
    return delegate.deg();
  }

  @Override
  public Field<BigDecimal> rad() {
    return delegate.rad();
  }

  @Override
  public Field<Integer> count() {
    return delegate.count();
  }

  @Override
  public Field<Integer> countDistinct() {
    return delegate.countDistinct();
  }

  @Override
  public Field<T> max() {
    return delegate.max();
  }

  @Override
  public Field<T> min() {
    return delegate.min();
  }

  @Override
  public Field<BigDecimal> sum() {
    return delegate.sum();
  }

  @Override
  public Field<BigDecimal> avg() {
    return delegate.avg();
  }

  @Override
  public Field<BigDecimal> median() {
    return delegate.median();
  }

  @Override
  public Field<BigDecimal> stddevPop() {
    return delegate.stddevPop();
  }

  @Override
  public Field<BigDecimal> stddevSamp() {
    return delegate.stddevSamp();
  }

  @Override
  public Field<BigDecimal> varPop() {
    return delegate.varPop();
  }

  @Override
  public Field<BigDecimal> varSamp() {
    return delegate.varSamp();
  }

  @Override
  public WindowPartitionByStep<Integer> countOver() {
    return delegate.countOver();
  }

  @Override
  public WindowPartitionByStep<T> maxOver() {
    return delegate.maxOver();
  }

  @Override
  public WindowPartitionByStep<T> minOver() {
    return delegate.minOver();
  }

  @Override
  public WindowPartitionByStep<BigDecimal> sumOver() {
    return delegate.sumOver();
  }

  @Override
  public WindowPartitionByStep<BigDecimal> avgOver() {
    return delegate.avgOver();
  }

  @Override
  public WindowIgnoreNullsStep<T> firstValue() {
    return delegate.firstValue();
  }

  @Override
  public WindowIgnoreNullsStep<T> lastValue() {
    return delegate.lastValue();
  }

  @Override
  public WindowIgnoreNullsStep<T> lead() {
    return delegate.lead();
  }

  @Override
  public WindowIgnoreNullsStep<T> lead(int offset) {
    return delegate.lead(offset);
  }

  @Override
  public WindowIgnoreNullsStep<T> lead(int offset, T defaultValue) {
    return delegate.lead(offset, defaultValue);
  }

  @Override
  public WindowIgnoreNullsStep<T> lead(int offset, Field<T> defaultValue) {
    return delegate.lead(offset, defaultValue);
  }

  @Override
  public WindowIgnoreNullsStep<T> lag() {
    return delegate.lag();
  }

  @Override
  public WindowIgnoreNullsStep<T> lag(int offset) {
    return delegate.lag(offset);
  }

  @Override
  public WindowIgnoreNullsStep<T> lag(int offset, T defaultValue) {
    return delegate.lag(offset, defaultValue);
  }

  @Override
  public WindowIgnoreNullsStep<T> lag(int offset, Field<T> defaultValue) {
    return delegate.lag(offset, defaultValue);
  }

  @Override
  public WindowPartitionByStep<BigDecimal> stddevPopOver() {
    return delegate.stddevPopOver();
  }

  @Override
  public WindowPartitionByStep<BigDecimal> stddevSampOver() {
    return delegate.stddevSampOver();
  }

  @Override
  public WindowPartitionByStep<BigDecimal> varPopOver() {
    return delegate.varPopOver();
  }

  @Override
  public WindowPartitionByStep<BigDecimal> varSampOver() {
    return delegate.varSampOver();
  }

  @Override
  public Field<String> upper() {
    return delegate.upper();
  }

  @Override
  public Field<String> lower() {
    return delegate.lower();
  }

  @Override
  public Field<String> trim() {
    return delegate.trim();
  }

  @Override
  public Field<String> rtrim() {
    return delegate.rtrim();
  }

  @Override
  public Field<String> ltrim() {
    return delegate.ltrim();
  }

  @Override
  public Field<String> rpad(Field<? extends Number> length) {
    return delegate.rpad(length);
  }

  @Override
  public Field<String> rpad(int length) {
    return delegate.rpad(length);
  }

  @Override
  public Field<String> rpad(Field<? extends Number> length, Field<String> character) {
    return delegate.rpad(length, character);
  }

  @Override
  public Field<String> rpad(int length, char character) {
    return delegate.rpad(length, character);
  }

  @Override
  public Field<String> lpad(Field<? extends Number> length) {
    return delegate.lpad(length);
  }

  @Override
  public Field<String> lpad(int length) {
    return delegate.lpad(length);
  }

  @Override
  public Field<String> lpad(Field<? extends Number> length, Field<String> character) {
    return delegate.lpad(length, character);
  }

  @Override
  public Field<String> lpad(int length, char character) {
    return delegate.lpad(length, character);
  }

  @Override
  public Field<String> repeat(Number count) {
    return delegate.repeat(count);
  }

  @Override
  public Field<String> repeat(Field<? extends Number> count) {
    return delegate.repeat(count);
  }

  @Override
  public Field<String> replace(Field<String> search) {
    return delegate.replace(search);
  }

  @Override
  public Field<String> replace(String search) {
    return delegate.replace(search);
  }

  @Override
  public Field<String> replace(Field<String> search, Field<String> replace) {
    return delegate.replace(search, replace);
  }

  @Override
  public Field<String> replace(String search, String replace) {
    return delegate.replace(search, replace);
  }

  @Override
  public Field<Integer> position(String search) {
    return delegate.position(search);
  }

  @Override
  public Field<Integer> position(Field<String> search) {
    return delegate.position(search);
  }

  @Override
  public Field<Integer> ascii() {
    return delegate.ascii();
  }

  @Override
  public Field<String> concat(Field<?>... fields) {
    return delegate.concat(fields);
  }

  @Override
  public Field<String> concat(String... values) {
    return delegate.concat(values);
  }

  @Override
  public Field<String> substring(int startingPosition) {
    return delegate.substring(startingPosition);
  }

  @Override
  public Field<String> substring(Field<? extends Number> startingPosition) {
    return delegate.substring(startingPosition);
  }

  @Override
  public Field<String> substring(int startingPosition, int length) {
    return delegate.substring(startingPosition, length);
  }

  @Override
  public Field<String> substring(Field<? extends Number> startingPosition,
      Field<? extends Number> length) {
    return delegate.substring(startingPosition, length);
  }

  @Override
  public Field<Integer> length() {
    return delegate.length();
  }

  @Override
  public Field<Integer> charLength() {
    return delegate.charLength();
  }

  @Override
  public Field<Integer> bitLength() {
    return delegate.bitLength();
  }

  @Override
  public Field<Integer> octetLength() {
    return delegate.octetLength();
  }

  @Override
  public Field<Integer> extract(DatePart datePart) {
    return delegate.extract(datePart);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Field<T> greatest(T... others) {
    return delegate.greatest(others);
  }

  @Override
  public Field<T> greatest(Field<?>... others) {
    return delegate.greatest(others);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Field<T> least(T... others) {
    return delegate.least(others);
  }

  @Override
  public Field<T> least(Field<?>... others) {
    return delegate.least(others);
  }

  @Override
  public Field<T> nvl(T defaultValue) {
    return delegate.nvl(defaultValue);
  }

  @Override
  public Field<T> nvl(Field<T> defaultValue) {
    return delegate.nvl(defaultValue);
  }

  @Override
  public <Z> Field<Z> nvl2(Z valueIfNotNull, Z valueIfNull) {
    return delegate.nvl2(valueIfNotNull, valueIfNull);
  }

  @Override
  public <Z> Field<Z> nvl2(Field<Z> valueIfNotNull, Field<Z> valueIfNull) {
    return delegate.nvl2(valueIfNotNull, valueIfNull);
  }

  @Override
  public Field<T> nullif(T other) {
    return delegate.nullif(other);
  }

  @Override
  public Field<T> nullif(Field<T> other) {
    return delegate.nullif(other);
  }

  @Override
  public <Z> Field<Z> decode(T search, Z result) {
    return delegate.decode(search, result);
  }

  @Override
  public <Z> Field<Z> decode(T search, Z result, Object... more) {
    return delegate.decode(search, result, more);
  }

  @Override
  public <Z> Field<Z> decode(Field<T> search, Field<Z> result) {
    return delegate.decode(search, result);
  }

  @Override
  public <Z> Field<Z> decode(Field<T> search, Field<Z> result, Field<?>... more) {
    return delegate.decode(search, result, more);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Field<T> coalesce(T option, T... options) {
    return delegate.coalesce(option, options);
  }

  @Override
  public Field<T> coalesce(Field<T> option, Field<?>... options) {
    return delegate.coalesce(option, options);
  }

}
