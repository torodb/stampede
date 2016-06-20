package com.torodb.backend.interfaces;

import javax.annotation.Nonnull;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.transaction.metainf.FieldType;

public interface DataTypeInterface {
    @Nonnull DataTypeForKV<?> getDataType(FieldType type);
}
