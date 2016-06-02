package com.torodb.backend.interfaces;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

public interface ReadInterface {
    @Nonnull String findDocsSelectStatement();
    void setFindDocsSelectStatementParameters(@Nonnull String schema, @Nonnull Integer[] requestedDocs,
            @Nonnull String[] paths, @Nonnull Connection connection, @Nonnull PreparedStatement ps) throws SQLException;
    @Nonnull ResultSet getFindDocsSelectStatementResultSet(PreparedStatement ps) throws SQLException;
    @Nonnull FindDocsSelectStatementRow getFindDocsSelectStatementRow(ResultSet rs) throws SQLException;
    
    public interface FindDocsSelectStatementRow {
        public int getDocId();
        public Integer getRowId();
        public Integer getParentRowId();
        public Integer getSequence();
        public String getJson();
        public boolean isRoot();
        public boolean isObject();
    }
}
