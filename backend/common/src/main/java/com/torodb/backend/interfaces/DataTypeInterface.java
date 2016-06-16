package com.torodb.backend.interfaces;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.core.transaction.metainf.FieldType;

public interface DataTypeInterface {
    @Nonnull ValueToJooqDataTypeProvider getValueToJooqDataTypeProvider();
    @Nonnull int getIntColumnType(ResultSet columns) throws SQLException;
    @Nonnull String getStringColumnType(ResultSet columns) throws SQLException;
    @Nonnull DataTypeForKV<?> getDataType(FieldType type);
}
