package com.torodb.core.backend;

import javax.annotation.Nonnull;

public interface IdentifierConstraints {
    int identifierMaxSize();
    boolean isAllowedSchemaIdentifier(@Nonnull String identifier);
    boolean isAllowedTableIdentifier(@Nonnull String identifier);
    boolean isAllowedColumnIdentifier(@Nonnull String identifier);
    boolean isSameIdentifier(@Nonnull String leftIdentifier, @Nonnull String rightIdentifier);
}
