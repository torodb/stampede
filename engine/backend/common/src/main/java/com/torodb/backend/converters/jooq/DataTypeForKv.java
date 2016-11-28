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

package com.torodb.backend.converters.jooq;

import com.torodb.kvdocument.values.KvValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public class DataTypeForKv<T extends KvValue<?>> implements DataType<T> {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <DT, JT, T extends KvValue<?>> DataTypeForKv<T> from(DataType<DT> dataType,
      KvValueConverter<DT, JT, T> converter) {
    return new DataTypeForKv<>(dataType.asConvertedDataType(new KvChainConverter(dataType
        .getConverter(), converter)), converter);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <DT, JT, T extends KvValue<?>> DataTypeForKv<T> from(DataType<DT> dataType,
      KvValueConverter<DT, JT, T> converter, int sqlType) {
    return new DataTypeForKv<>(dataType.asConvertedDataType(new KvChainConverter(dataType
        .getConverter(), converter)), converter, sqlType);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <DT, JT, T extends KvValue<?>> DataTypeForKv<T> from(DataType<DT> dataType,
      KvValueConverter<DT, JT, T> converter, Binding<DT, T> binding) {
    return new DataTypeForKv<>(dataType.asConvertedDataType(new KvChainBinding(binding, dataType
        .getConverter(), converter)), converter);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <DT, JT, T extends KvValue<?>> DataTypeForKv<T> from(DataType<DT> dataType,
      KvValueConverter<DT, JT, T> converter, Binding<DT, T> binding, int sqlType) {
    return new DataTypeForKv<>(dataType.asConvertedDataType(new KvChainBinding(binding, dataType
        .getConverter(), converter)), converter, sqlType);
  }

  private final DataType<T> dataType;
  private final int sqlType;
  private final KvValueConverter<?, ?, T> kvValueConverter;

  private DataTypeForKv(DataType<T> dataType, KvValueConverter<?, ?, T> kvValueConverter) {
    super();
    this.dataType = dataType;
    this.sqlType = dataType.getSQLType();
    this.kvValueConverter = kvValueConverter;
  }

  private DataTypeForKv(DataType<T> dataType, KvValueConverter<?, ?, T> kvValueConverter,
      int sqlType) {
    super();
    this.dataType = dataType;
    this.sqlType = sqlType;
    this.kvValueConverter = kvValueConverter;
  }

  public KvValueConverter<?, ?, T> getKvValueConverter() {
    return kvValueConverter;
  }

  @SuppressFBWarnings(value = "NM_CONFUSING", justification = "we cannot "
      + "change the name of a jOOQ method. And it goes against the code"
      + "style")
  @Override
  public int getSQLType() {
    return sqlType;
  }

  @Override
  public DataType<T> getSQLDataType() {
    return dataType.getSQLDataType();
  }

  @Override
  public DataType<T> getDataType(Configuration configuration) {
    return dataType.getDataType(configuration);
  }

  @Override
  public Binding<?, T> getBinding() {
    return dataType.getBinding();
  }

  @Override
  public Converter<?, T> getConverter() {
    return dataType.getConverter();
  }

  @Override
  public Class<T> getType() {
    return dataType.getType();
  }

  @Override
  public Class<T[]> getArrayType() {
    return dataType.getArrayType();
  }

  @Override
  public DataType<T[]> getArrayDataType() {
    return dataType.getArrayDataType();
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <E extends EnumType> DataType<E> asEnumDataType(Class<E> enumDataType) {
    DataType<E> dataType = this.dataType.asEnumDataType(enumDataType);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <U> DataType<U> asConvertedDataType(Converter<? super T, U> converter) {
    DataType dataType = this.dataType.asConvertedDataType(converter);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <U> DataType<U> asConvertedDataType(Binding<? super T, U> binding) {
    DataType dataType = this.dataType.asConvertedDataType(binding);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  public String getTypeName() {
    return dataType.getTypeName();
  }

  @Override
  public String getTypeName(Configuration configuration) {
    return dataType.getTypeName(configuration);
  }

  @Override
  public String getCastTypeName() {
    return dataType.getCastTypeName();
  }

  @Override
  public String getCastTypeName(Configuration configuration) {
    return dataType.getCastTypeName(configuration);
  }

  @Override
  public SQLDialect getDialect() {
    return dataType.getDialect();
  }

  @Override
  public T convert(Object object) {
    return dataType.convert(object);
  }

  @Override
  public T[] convert(Object... objects) {
    return dataType.convert(objects);
  }

  @Override
  public List<T> convert(Collection<?> objects) {
    return dataType.convert(objects);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public DataType<T> nullable(boolean nullable) {
    DataType dataType = this.dataType.nullable(nullable);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  public boolean nullable() {
    return dataType.nullable();
  }

  @Override
  @Deprecated
  public DataType<T> defaulted(boolean defaulted) {
    return dataType.defaulted(defaulted);
  }

  @Override
  public boolean defaulted() {
    return dataType.defaulted();
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public DataType<T> precision(int precision) {
    DataType dataType = this.dataType.precision(precision);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public DataType<T> precision(int precision, int scale) {
    DataType dataType = this.dataType.precision(precision, scale);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  public int precision() {
    return dataType.precision();
  }

  @Override
  public boolean hasPrecision() {
    return dataType.hasPrecision();
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public DataType<T> scale(int scale) {
    DataType dataType = this.dataType.scale(scale);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  public int scale() {
    return dataType.scale();
  }

  @Override
  public boolean hasScale() {
    return dataType.hasScale();
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public DataType<T> length(int length) {
    DataType dataType = this.dataType.length(length);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  public int length() {
    return dataType.length();
  }

  @Override
  public boolean hasLength() {
    return dataType.hasLength();
  }

  @Override
  public boolean isNumeric() {
    return dataType.isNumeric();
  }

  @Override
  public boolean isString() {
    return dataType.isString();
  }

  @Override
  public boolean isDateTime() {
    return dataType.isDateTime();
  }

  @Override
  public boolean isTemporal() {
    return dataType.isTemporal();
  }

  @Override
  public boolean isInterval() {
    return dataType.isInterval();
  }

  @Override
  public boolean isBinary() {
    return dataType.isBinary();
  }

  @Override
  public boolean isLob() {
    return dataType.isLob();
  }

  @Override
  public boolean isArray() {
    return dataType.isArray();
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public DataType<T> defaultValue(T defaultValue) {
    DataType dataType = this.dataType.defaultValue(defaultValue);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public DataType<T> defaultValue(Field<T> defaultValue) {
    DataType dataType = this.dataType.defaultValue(defaultValue);
    return new DataTypeForKv(dataType, kvValueConverter);
  }

  @Override
  public Field<T> defaultValue() {
    return dataType.defaultValue();
  }

  public static class KvChainConverter<NewT, ChainT, WrappedT>
      implements Converter<NewT, WrappedT> {

    private static final long serialVersionUID = 1L;

    private final Converter<NewT, ChainT> leftConverter;
    private final Converter<ChainT, WrappedT> rightConverter;

    public KvChainConverter(Converter<NewT, ChainT> leftConverter,
        Converter<ChainT, WrappedT> rightConverter) {
      super();
      this.leftConverter = leftConverter;
      this.rightConverter = rightConverter;
    }

    @Override
    public WrappedT from(NewT databaseObject) {
      return rightConverter.from(leftConverter.from(databaseObject));
    }

    @Override
    public NewT to(WrappedT userObject) {
      if (userObject == null) {
        return null;
      }
      return leftConverter.to(rightConverter.to(userObject));
    }

    @Override
    public Class<NewT> fromType() {
      return leftConverter.fromType();
    }

    @Override
    public Class<WrappedT> toType() {
      return rightConverter.toType();
    }
  }

  public static class KvChainBinding<NewT, ChainT, WrappedT> implements Binding<NewT, WrappedT> {

    private static final long serialVersionUID = 1L;

    private final Binding<NewT, WrappedT> delegate;
    private final KvChainConverter<NewT, ChainT, WrappedT> chainConverter;

    public KvChainBinding(Binding<NewT, WrappedT> delegate, Converter<NewT, ChainT> leftConverter,
        Converter<ChainT, WrappedT> rightConverter) {
      super();
      this.delegate = delegate;
      this.chainConverter = new KvChainConverter<>(leftConverter, rightConverter);
    }

    public Converter<NewT, WrappedT> converter() {
      return chainConverter;
    }

    public void sql(BindingSQLContext<WrappedT> ctx) throws SQLException {
      delegate.sql(ctx);
    }

    public void register(BindingRegisterContext<WrappedT> ctx) throws SQLException {
      delegate.register(ctx);
    }

    public void set(BindingSetStatementContext<WrappedT> ctx) throws SQLException {
      delegate.set(ctx);
    }

    public void set(BindingSetSQLOutputContext<WrappedT> ctx) throws SQLException {
      delegate.set(ctx);
    }

    public void get(BindingGetResultSetContext<WrappedT> ctx) throws SQLException {
      delegate.get(ctx);
    }

    public void get(BindingGetStatementContext<WrappedT> ctx) throws SQLException {
      delegate.get(ctx);
    }

    public void get(BindingGetSQLInputContext<WrappedT> ctx) throws SQLException {
      delegate.get(ctx);
    }
  }
}
