
package com.torodb.torod.db.backends.sql.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.values.NullValue;
import com.torodb.torod.core.subdocument.values.Value;
import com.torodb.torod.db.backends.converters.BasicTypeToSqlType;
import com.torodb.torod.db.backends.converters.jooq.SubdocValueConverter;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqConverterProvider;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SqlWindow implements Iterator<ValueRow<Value>> {

    private final Iterator<ValueRow<Value>> rows;

    public SqlWindow(ResultSet rs, BasicTypeToSqlType basicTypeToSqlType) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        final int columnLenght = metaData.getColumnCount();
        
        String[] columns = new String[columnLenght];
        SubdocValueConverter[] converters = new SubdocValueConverter[columnLenght];

        for (int i = 0; i < columnLenght; i++) {
            
            int jdbcIndex = i + 1; //result sets are 1-based instead of 0-based!
            columns[i] = metaData.getColumnName(jdbcIndex);

            BasicType basicType = getBasicType(metaData, jdbcIndex, basicTypeToSqlType);

            converters[i] = ValueToJooqConverterProvider.getConverter(basicType);
        }

        ImmutableList.Builder<ValueRow<Value>> builder = ImmutableList.builder();

        while (rs.next()) {
            Value[] values = new Value[columns.length];
            for (int i = 0; i < columns.length; i++) {
                int jdbcIndex = i+1;

                Value value = readAndTransform(rs, jdbcIndex, converters[i]);
                Preconditions.checkNotNull(value);
                values[i] = value;
            }
            builder.add(new WindowValueRow(columns, values));
        }
        rows = builder.build().iterator();
    }

    @Override
    public boolean hasNext() {
        return rows.hasNext();
    }

    @Override
    public ValueRow<Value> next() {
        return rows.next();
    }

    @Override
    public void remove() {
        rows.remove();
    }

    @Nonnull
    private static Value readAndTransform(
            ResultSet rs,
            int jdbcIndex,
            SubdocValueConverter converter) throws SQLException {
        Object value;
        switch (converter.getErasuredType()) {
            case BOOLEAN:
                value = rs.getBoolean(jdbcIndex);
                break;
            case DATETIME:
                value = rs.getTimestamp(jdbcIndex);
                break;
            case DATE:
                value = rs.getDate(jdbcIndex);
                break;
            case DOUBLE:
                value = rs.getDouble(jdbcIndex);
                break;
            case INTEGER:
                value = rs.getInt(jdbcIndex);
                break;
            case LONG:
                value = rs.getLong(jdbcIndex);
                break;
            case NULL:
                return NullValue.INSTANCE;
            case ARRAY:
            case PATTERN:
            case STRING:
                value = rs.getString(jdbcIndex);
                break;

            case TIME:
                value = rs.getTime(jdbcIndex);
                break;
            case TWELVE_BYTES:
                value = rs.getBytes(jdbcIndex);
                break;
            default:
                throw new AssertionError("Unexpected basic type " + converter.getErasuredType());
        }
        if (value == null) {
            return NullValue.INSTANCE;
        }
        return (Value) converter.from(value);
    }

    private static BasicType getBasicType(
            ResultSetMetaData metaData, int jdbcIndex, BasicTypeToSqlType basicTypeToSqlType
    ) throws SQLException {
        int columnType = metaData.getColumnType(jdbcIndex);
        String columnTypeName = metaData.getColumnTypeName(jdbcIndex);
        String columnName = metaData.getColumnName(jdbcIndex);

        try {
            return basicTypeToSqlType.toBasicType(
                    columnName,
                    columnType,
                    columnTypeName
            );
        } catch (ToroImplementationException ex) {
            if (columnTypeName.equals("bytea")) {
                return BasicType.TWELVE_BYTES;
            }
            else {
                throw ex;
            }
        }
    }

    private static class WindowValueRow implements ValueRow<Value> {

        private final String[] columns;
        private final Value[] values;

        private WindowValueRow(@Nonnull String[] columns, @Nonnull Value[] values) {
            this.columns = columns;
            this.values = values;

            Preconditions.checkArgument(columns.length == values.length);
        }

        @Override
        public Set<Map.Entry<String, Value>> entrySet() {
            HashSet<Map.Entry<String, Value>> result = new HashSet<>();
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                Value value = values[i];
                result.add(new SimpleEntry<>(column, value));
            }
            return result;
        }

        @Override
        public Set<String> keySet() {
            return Sets.newHashSet(Iterators.forArray(columns));
        }

        @Override
        public Set<Value> valueSet() {
            return Sets.newHashSet(Iterators.forArray(values));
        }

        @Override
        public Value get(String key) throws IllegalArgumentException {
            int searchResult = -1;
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equals(key)) {
                    searchResult = i;
                }
            }
            if (searchResult < 0) {
                throw new IllegalArgumentException("The input '" + key + "' is not contained on this row");
            }
            return values[searchResult];
        }

        @Override
        public void consume(ForEachConsumer<Value> consumer) {
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                Value value = values[i];

                consumer.consume(column, value);
            }
        }

    }

}
