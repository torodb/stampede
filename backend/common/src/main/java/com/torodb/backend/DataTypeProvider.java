package com.torodb.backend;

import javax.annotation.Nonnull;

import org.jooq.SQLDialect;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.transaction.metainf.FieldType;

public interface DataTypeProvider {
    @Nonnull DataTypeForKV<?> getDataType(FieldType type);
    @Nonnull SQLDialect getDialect();
}
