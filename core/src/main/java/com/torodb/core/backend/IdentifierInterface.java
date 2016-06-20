package com.torodb.core.backend;

import javax.annotation.Nonnull;

import com.torodb.core.transaction.metainf.FieldType;

public interface IdentifierInterface {
    int identifierMaxSize();
    boolean isAllowedSchemaIdentifier(@Nonnull String identifier);
    boolean isAllowedTableIdentifier(@Nonnull String identifier);
    boolean isAllowedColumnIdentifier(@Nonnull String identifier);
    boolean isSameIdentifier(@Nonnull String leftIdentifier, @Nonnull String rightIdentifier);
    char getSeparator();
    char getArrayDimensionSeparator();
    @Nonnull char getFieldTypeIdentifier(@Nonnull FieldType fieldType);
    @Nonnull String getScalarIdentifier(@Nonnull FieldType fieldType);
}
