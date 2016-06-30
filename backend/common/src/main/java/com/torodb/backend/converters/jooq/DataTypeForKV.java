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

package com.torodb.backend.converters.jooq;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.jooq.Binding;
import org.jooq.BindingGetResultSetContext;
import org.jooq.BindingGetSQLInputContext;
import org.jooq.BindingGetStatementContext;
import org.jooq.BindingRegisterContext;
import org.jooq.BindingSQLContext;
import org.jooq.BindingSetSQLOutputContext;
import org.jooq.BindingSetStatementContext;
import org.jooq.Configuration;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.SQLDialect;

import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
public class DataTypeForKV<T extends KVValue<?>> implements DataType<T> {
    
    private static final long serialVersionUID = 1L;
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <DT, JT, T extends KVValue<?>> DataTypeForKV<T> from(DataType<DT> dataType, KVValueConverter<DT, JT, T> converter) {
        return new DataTypeForKV<>(dataType.asConvertedDataType(new KVChainConverter(dataType.getConverter(), converter)), converter);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <DT, JT, T extends KVValue<?>> DataTypeForKV<T> from(DataType<DT> dataType, KVValueConverter<DT, JT, T> converter, int sqlType) {
        return new DataTypeForKV<>(dataType.asConvertedDataType(new KVChainConverter(dataType.getConverter(), converter)), converter, sqlType);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <DT, JT, T extends KVValue<?>> DataTypeForKV<T> from(DataType<DT> dataType, KVValueConverter<DT, JT, T> converter, Binding<DT, T> binding) {
        return new DataTypeForKV<>(dataType.asConvertedDataType(new KVChainBinding(binding, dataType.getConverter(), converter)), converter);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <DT, JT, T extends KVValue<?>> DataTypeForKV<T> from(DataType<DT> dataType, KVValueConverter<DT, JT, T> converter, Binding<DT, T> binding, int sqlType) {
        return new DataTypeForKV<>(dataType.asConvertedDataType(new KVChainBinding(binding, dataType.getConverter(), converter)), converter, sqlType);
    }
    
    private final DataType<T> dataType;
    private final int sqlType;
    private final KVValueConverter<?, ?, T> kvValueConverter;
    
    private DataTypeForKV(DataType<T> dataType, KVValueConverter<?, ?, T> kvValueConverter) {
        super();
        this.dataType = dataType;
        this.sqlType = dataType.getSQLType();
        this.kvValueConverter = kvValueConverter;
    }
    
    private DataTypeForKV(DataType<T> dataType, KVValueConverter<?, ?, T> kvValueConverter, int sqlType) {
        super();
        this.dataType = dataType;
        this.sqlType = sqlType;
        this.kvValueConverter = kvValueConverter;
    }
    
    public KVValueConverter<?, ?, T> getKVValueConverter() {
        return kvValueConverter;
    }

    public int getSQLType() {
        return sqlType;
    }
    
    public DataType<T> getSQLDataType() {
        return dataType.getSQLDataType();
    }

    public DataType<T> getDataType(Configuration configuration) {
        return dataType.getDataType(configuration);
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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <E extends EnumType> DataType<E> asEnumDataType(Class<E> enumDataType) {
        DataType<E> dataType = this.dataType.asEnumDataType(enumDataType);
        return new DataTypeForKV(dataType, kvValueConverter);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <U> DataType<U> asConvertedDataType(Converter<? super T, U> converter) {
        DataType dataType = this.dataType.asConvertedDataType(converter);
        return new DataTypeForKV(dataType, kvValueConverter);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <U> DataType<U> asConvertedDataType(Binding<? super T, U> binding) {
        DataType dataType = this.dataType.asConvertedDataType(binding);
        return new DataTypeForKV(dataType, kvValueConverter);
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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataType<T> nullable(boolean nullable) {
        DataType dataType = this.dataType.nullable(nullable);
        return new DataTypeForKV(dataType, kvValueConverter);
    }

    public boolean nullable() {
        return dataType.nullable();
    }

    @Deprecated
    public DataType<T> defaulted(boolean defaulted) {
        return dataType.defaulted(defaulted);
    }

    public boolean defaulted() {
        return dataType.defaulted();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataType<T> precision(int precision) {
        DataType dataType = this.dataType.precision(precision);
        return new DataTypeForKV(dataType, kvValueConverter);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataType<T> precision(int precision, int scale) {
        DataType dataType = this.dataType.precision(precision, scale);
        return new DataTypeForKV(dataType, kvValueConverter);
    }

    public int precision() {
        return dataType.precision();
    }

    public boolean hasPrecision() {
        return dataType.hasPrecision();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataType<T> scale(int scale) {
        DataType dataType = this.dataType.scale(scale);
        return new DataTypeForKV(dataType, kvValueConverter);
    }

    public int scale() {
        return dataType.scale();
    }

    public boolean hasScale() {
        return dataType.hasScale();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataType<T> length(int length) {
        DataType dataType = this.dataType.length(length);
        return new DataTypeForKV(dataType, kvValueConverter);
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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataType<T> defaultValue(T defaultValue) {
        DataType dataType = this.dataType.defaultValue(defaultValue);
        return new DataTypeForKV(dataType, kvValueConverter);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataType<T> defaultValue(Field<T> defaultValue) {
        DataType dataType = this.dataType.defaultValue(defaultValue);
        return new DataTypeForKV(dataType, kvValueConverter);
    }

    public Field<T> defaultValue() {
        return dataType.defaultValue();
    }
    
    public static class KVChainConverter<NewT, ChainT, WrappedU> implements Converter<NewT, WrappedU> {

        private static final long serialVersionUID = 1L;
        
        private final Converter<NewT, ChainT> leftConverter;
        private final Converter<ChainT, WrappedU> rightConverter;
        
        public KVChainConverter(Converter<NewT, ChainT> leftConverter, Converter<ChainT, WrappedU> rightConverter) {
            super();
            this.leftConverter = leftConverter;
            this.rightConverter = rightConverter;
        }

        @Override
        public WrappedU from(NewT databaseObject) {
            return rightConverter.from(leftConverter.from(databaseObject));
        }

        @Override
        public NewT to(WrappedU userObject) {
            if (userObject == null) return null;
            return leftConverter.to(rightConverter.to(userObject));
        }

        @Override
        public Class<NewT> fromType() {
            return leftConverter.fromType();
        }

        @Override
        public Class<WrappedU> toType() {
            return rightConverter.toType();
        }
    }
    
    public static class KVChainBinding<NewT, ChainT, WrappedU> implements Binding<NewT, WrappedU> {

        private static final long serialVersionUID = 1L;
        
        private final Binding<NewT, WrappedU> delegate;
        private final KVChainConverter<NewT, ChainT, WrappedU> chainConverter;
        
        public KVChainBinding(Binding<NewT, WrappedU> delegate, Converter<NewT, ChainT> leftConverter, Converter<ChainT, WrappedU> rightConverter) {
            super();
            this.delegate = delegate;
            this.chainConverter = new KVChainConverter<>(leftConverter, rightConverter);
        }
        
        public Converter<NewT, WrappedU> converter() {
            return chainConverter;
        }

        public void sql(BindingSQLContext<WrappedU> ctx) throws SQLException {
            delegate.sql(ctx);
        }

        public void register(BindingRegisterContext<WrappedU> ctx) throws SQLException {
            delegate.register(ctx);
        }

        public void set(BindingSetStatementContext<WrappedU> ctx) throws SQLException {
            delegate.set(ctx);
        }

        public void set(BindingSetSQLOutputContext<WrappedU> ctx) throws SQLException {
            delegate.set(ctx);
        }

        public void get(BindingGetResultSetContext<WrappedU> ctx) throws SQLException {
            delegate.get(ctx);
        }

        public void get(BindingGetStatementContext<WrappedU> ctx) throws SQLException {
            delegate.get(ctx);
        }

        public void get(BindingGetSQLInputContext<WrappedU> ctx) throws SQLException {
            delegate.get(ctx);
        }
    }
}