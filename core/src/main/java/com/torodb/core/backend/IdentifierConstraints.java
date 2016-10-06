package com.torodb.core.backend;

import com.torodb.core.transaction.metainf.FieldType;
import javax.annotation.Nonnull;

public interface IdentifierConstraints {
    int identifierMaxSize();
    boolean isAllowedSchemaIdentifier(@Nonnull String identifier);
    boolean isAllowedTableIdentifier(@Nonnull String identifier);
    boolean isAllowedColumnIdentifier(@Nonnull String identifier);
    boolean isAllowedIndexIdentifier(@Nonnull String identifier);
    boolean isSameIdentifier(@Nonnull String leftIdentifier, @Nonnull String rightIdentifier);
    char getSeparator();
    char getArrayDimensionSeparator();
    char getFieldTypeIdentifier(FieldType fieldType);
    String getScalarIdentifier(FieldType fieldType);
}
