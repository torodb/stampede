package com.torodb.backend.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.Sequence;
import org.jooq.UDT;
import org.jooq.impl.SchemaImpl;

import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.SqlInterface;

public final class DatabaseSchema extends SchemaImpl {

    private static final long serialVersionUID = 577805060;

    private final String database;
    private final SqlInterface databaseInterface;

    public DatabaseSchema(
            @Nonnull String database,
            @Nonnull String schemaName,
            SqlInterface databaseInterface
    ) throws InvalidDatabaseSchemaException {
        super(schemaName);

        this.database = database;
        this.databaseInterface = databaseInterface;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public final List<Sequence<?>> getSequences() {
        return Collections.<Sequence<?>>emptyList();
    }

    @Override
    public final List<UDT<?>> getUDTs() {
        List<UDT<?>> result = new ArrayList<>();
        result.addAll(getUDTs0());
        return result;
    }

    private List<UDT<?>> getUDTs0() {
        return Collections.emptyList();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public SqlInterface getDatabaseInterface() {
        return databaseInterface;
    }
}
