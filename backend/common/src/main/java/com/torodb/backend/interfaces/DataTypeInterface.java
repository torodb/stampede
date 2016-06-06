package com.torodb.backend.interfaces;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.backend.mocks.KVTypeToSqlType;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;

public interface DataTypeInterface {
    @Nonnull ValueToJooqDataTypeProvider getValueToJooqDataTypeProvider();
    
    @Nonnull String escapeSchemaName(@Nonnull String collection) throws IllegalArgumentException;
    @Nonnull String escapeAttributeName(@Nonnull String attributeName) throws IllegalArgumentException;
    @Nonnull String escapeIndexName(@Nonnull String indexName) throws IllegalArgumentException;
    boolean isSameIdentifier(@Nonnull String leftIdentifier, @Nonnull String rightIdentifier);

    @Nonnull int getIntColumnType(ResultSet columns) throws SQLException;
    @Nonnull String getStringColumnType(ResultSet columns) throws SQLException;
    @Nonnull KVTypeToSqlType getKVTypeToSqlType();
    @Nonnull DataTypeForKV<?> getDataType(FieldType type);
}
