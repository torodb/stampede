package com.torodb.backend.interfaces;

import com.torodb.core.d2r.DocPartData;
import java.sql.Connection;
import java.util.List;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;

public interface WriteInterface {
    Connection createWriteConnection();

    void insertDocPartData(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull DocPartData docPartData);
    void deleteDocParts(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull List<Integer> dids);
}
