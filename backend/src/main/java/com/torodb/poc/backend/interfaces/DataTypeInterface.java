package com.torodb.poc.backend.interfaces;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import org.jooq.DataType;

import com.torodb.kvdocument.types.KVType;
import com.torodb.poc.backend.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.poc.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.poc.backend.mocks.KVTypeToSqlType;

public interface DataTypeInterface {
    @Nonnull ValueToJooqConverterProvider getValueToJooqConverterProvider();
    @Nonnull ValueToJooqDataTypeProvider getValueToJooqDataTypeProvider();
    
    @Nonnull String escapeSchemaName(@Nonnull String collection) throws IllegalArgumentException;
    @Nonnull String escapeAttributeName(@Nonnull String attributeName) throws IllegalArgumentException;
    @Nonnull String escapeIndexName(@Nonnull String indexName) throws IllegalArgumentException;
    
    @Nonnull int getIntColumnType(ResultSet columns) throws SQLException;
    @Nonnull String getStringColumnType(ResultSet columns) throws SQLException;
    @Nonnull KVTypeToSqlType getKVTypeToSqlType();
    @Nonnull DataType<?> getDataType(KVType type);
    @Nonnull DataType<?> getDataType(String type);
}
