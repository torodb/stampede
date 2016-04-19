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

package com.torodb.torod.db.backends.converters.jooq;

import java.util.Collection;
import java.util.List;

import org.jooq.Binding;
import org.jooq.Configuration;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.EnumType;
import org.jooq.SQLDialect;

import com.torodb.torod.core.subdocument.values.ScalarValue;

/**
 *
 */
public class DataTypeForScalar<T extends ScalarValue<?>> implements DataType<T> {
    
    private static final long serialVersionUID = 1L;
    
    public static <DT, T extends ScalarValue<?>> DataTypeForScalar<T> from(DataType<DT> dataType, SubdocValueConverter<DT, T> converter) {
        return new DataTypeForScalar<>(dataType.asConvertedDataType(converter), converter);
    }
    
    public static <DT, T extends ScalarValue<?>> DataTypeForScalar<T> from(DataType<DT> dataType, SubdocValueConverter<DT, T> converter, Binding<DT, T> binding) {
        return new DataTypeForScalar<>(dataType.asConvertedDataType(binding), converter);
    }
    
    private final DataType<T> dataType;
    private final SubdocValueConverter<?, T> subdocValueConverter;
    
    private DataTypeForScalar(DataType<T> dataType, SubdocValueConverter<?, T> subdocValueConverter) {
        super();
        this.dataType = dataType;
        this.subdocValueConverter = subdocValueConverter;
    }

    public SubdocValueConverter<?, T> getSubdocValueConverter() {
        return subdocValueConverter;
    }
    
    public DataType<T> getSQLDataType() {
        return dataType.getSQLDataType();
    }

    public DataType<T> getDataType(Configuration configuration) {
        return dataType.getDataType(configuration);
    }

    public int getSQLType() {
        return dataType.getSQLType();
    }

    public Binding<?, T> getBinding() {
        return dataType.getBinding();
    }

    public Converter<?, T> getConverter() {
        return dataType.getConverter();
    }

    public Class<T> getType() {
        return dataType.getType();
    }

    public Class<T[]> getArrayType() {
        return dataType.getArrayType();
    }

    public DataType<T[]> getArrayDataType() {
        return dataType.getArrayDataType();
    }

    public <E extends EnumType> DataType<E> asEnumDataType(Class<E> enumDataType) {
        return dataType.asEnumDataType(enumDataType);
    }

    public <U> DataType<U> asConvertedDataType(Converter<? super T, U> converter) {
        return dataType.asConvertedDataType(converter);
    }

    public <U> DataType<U> asConvertedDataType(Binding<? super T, U> binding) {
        return dataType.asConvertedDataType(binding);
    }

    public String getTypeName() {
        return dataType.getTypeName();
    }

    public String getTypeName(Configuration configuration) {
        return dataType.getTypeName(configuration);
    }

    public String getCastTypeName() {
        return dataType.getCastTypeName();
    }

    public String getCastTypeName(Configuration configuration) {
        return dataType.getCastTypeName(configuration);
    }

    public SQLDialect getDialect() {
        return dataType.getDialect();
    }

    public T convert(Object object) {
        return dataType.convert(object);
    }

    public T[] convert(Object... objects) {
        return dataType.convert(objects);
    }

    public List<T> convert(Collection<?> objects) {
        return dataType.convert(objects);
    }

    public DataType<T> nullable(boolean nullable) {
        return dataType.nullable(nullable);
    }

    public boolean nullable() {
        return dataType.nullable();
    }

    public DataType<T> defaulted(boolean defaulted) {
        return dataType.defaulted(defaulted);
    }

    public boolean defaulted() {
        return dataType.defaulted();
    }

    public DataType<T> precision(int precision) {
        return dataType.precision(precision);
    }

    public DataType<T> precision(int precision, int scale) {
        return dataType.precision(precision, scale);
    }

    public int precision() {
        return dataType.precision();
    }

    public boolean hasPrecision() {
        return dataType.hasPrecision();
    }

    public DataType<T> scale(int scale) {
        return dataType.scale(scale);
    }

    public int scale() {
        return dataType.scale();
    }

    public boolean hasScale() {
        return dataType.hasScale();
    }

    public DataType<T> length(int length) {
        return dataType.length(length);
    }

    public int length() {
        return dataType.length();
    }

    public boolean hasLength() {
        return dataType.hasLength();
    }

    public boolean isNumeric() {
        return dataType.isNumeric();
    }

    public boolean isString() {
        return dataType.isString();
    }

    public boolean isDateTime() {
        return dataType.isDateTime();
    }

    public boolean isTemporal() {
        return dataType.isTemporal();
    }

    public boolean isInterval() {
        return dataType.isInterval();
    }

    public boolean isBinary() {
        return dataType.isBinary();
    }

    public boolean isLob() {
        return dataType.isLob();
    }

    public boolean isArray() {
        return dataType.isArray();
    }
}