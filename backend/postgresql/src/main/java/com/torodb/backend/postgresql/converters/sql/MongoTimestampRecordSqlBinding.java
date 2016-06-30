package com.torodb.backend.postgresql.converters.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.util.PGobject;

import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.udt.MongoTimestampUDT;
import com.torodb.backend.udt.record.MongoTimestampRecord;

public class MongoTimestampRecordSqlBinding implements SqlBinding<MongoTimestampRecord> {
    
    public static final MongoTimestampRecordSqlBinding INSTANCE =
            new MongoTimestampRecordSqlBinding();
    
    @Override
    public MongoTimestampRecord get(ResultSet resultSet, int columnIndex) throws SQLException {
        PGobject pgObject = (PGobject) resultSet.getObject(columnIndex);
        
        if (pgObject == null) {
            return null;
        }
        
        String value = pgObject.getValue();
        int indexOfComma = value.indexOf(',');
        Integer secs = Integer.parseInt(value.substring(1, indexOfComma));
        Integer count = Integer.parseInt(value.substring(indexOfComma + 1, value.length() - 1));
        return new MongoTimestampRecord(secs, count);
    }

    @Override
    public void set(PreparedStatement preparedStatement, int parameterIndex, MongoTimestampRecord value)
            throws SQLException {
        preparedStatement.setString(parameterIndex, "(" + value.getSecs() + ',' + value.getCounter() + ')');
    }
    
    @Override
    public String getPlaceholder() {
        return "?::\"" + TorodbSchema.IDENTIFIER + "\".\"" + MongoTimestampUDT.IDENTIFIER + '"';
    }
}
