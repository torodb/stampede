
package com.torodb.torod.db.backends.sql.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarNull;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.db.backends.converters.ScalarTypeToSqlType;
import com.torodb.torod.db.backends.converters.jooq.SubdocValueConverter;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.torod.db.backends.udt.records.MongoTimestampRecord;

/**
 *
 */
public class SqlWindow implements Iterator<ValueRow<ScalarValue<?>>> {

    private final Iterator<ValueRow<ScalarValue<?>>> rows;

    public SqlWindow(ResultSet rs, 
            ValueToJooqConverterProvider valueToJooqConverterProvider, 
            ValueToJooqDataTypeProvider valueToJooqDataTypeProvider, 
            ScalarTypeToSqlType scalarTypeToSqlType) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        final int columnLenght = metaData.getColumnCount();
        
        String[] columns = new String[columnLenght];
        SubdocValueConverter<?, ?>[] converters = new SubdocValueConverter[columnLenght];

        for (int i = 0; i < columnLenght; i++) {
            
            int jdbcIndex = i + 1; //result sets are 1-based instead of 0-based!
            columns[i] = metaData.getColumnName(jdbcIndex);

            ScalarType scalarType = getScalarType(metaData, jdbcIndex, scalarTypeToSqlType);

            converters[i] = valueToJooqDataTypeProvider.getDataType(scalarType).getSubdocValueConverter();
        }

        ImmutableList.Builder<ValueRow<ScalarValue<?>>> builder = ImmutableList.builder();

        while (rs.next()) {
            ScalarValue[] values = new ScalarValue[columns.length];
            for (int i = 0; i < columns.length; i++) {
                int jdbcIndex = i+1;

                ScalarValue<?> value = readAndTransform(rs, jdbcIndex, converters[i]);
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
    public ValueRow<ScalarValue<?>> next() {
        return rows.next();
    }

    @Override
    public void remove() {
        rows.remove();
    }

    @Nonnull
    private static <DBT, V extends ScalarValue<?>> ScalarValue<?> readAndTransform(
            ResultSet rs,
            int jdbcIndex,
            SubdocValueConverter<DBT, V> converter) throws SQLException {
        Object value;
        switch (converter.getErasuredType()) {
            case BOOLEAN:
                value = rs.getBoolean(jdbcIndex);
                break;
            case INSTANT:
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
                return ScalarNull.getInstance();
            case ARRAY:
            case STRING:
                value = rs.getString(jdbcIndex);
                break;

            case TIME:
                value = rs.getTime(jdbcIndex);
                break;
            case MONGO_OBJECT_ID:
                value = rs.getBytes(jdbcIndex);
                break;
            case MONGO_TIMESTAMP:
                value = rs.getObject(jdbcIndex, MongoTimestampRecord.class);
                break;
            default:
                throw new AssertionError("Unexpected scalar type " + converter.getErasuredType());
        }
        if (value == null) {
            return ScalarNull.getInstance();
        }
        return converter.from((DBT) value);
    }

    private static ScalarType getScalarType(
            ResultSetMetaData metaData, int jdbcIndex, ScalarTypeToSqlType scalarTypeToSqlType
    ) throws SQLException {
        int columnType = metaData.getColumnType(jdbcIndex);
        String columnTypeName = metaData.getColumnTypeName(jdbcIndex);
        String columnName = metaData.getColumnName(jdbcIndex);

        try {
            return scalarTypeToSqlType.toScalarType(
                    columnName,
                    columnType,
                    columnTypeName
            );
        } catch (ToroImplementationException ex) {
            if (columnTypeName.equals("bytea")) {
                return ScalarType.MONGO_OBJECT_ID;
            }
            else {
                throw ex;
            }
        }
    }

    private static class WindowValueRow implements ValueRow<ScalarValue<?>> {

        private final String[] columns;
        private final ScalarValue<?>[] values;

        private WindowValueRow(@Nonnull String[] columns, @Nonnull ScalarValue[] values) {
            this.columns = columns;
            this.values = values;

            Preconditions.checkArgument(columns.length == values.length);
        }

        @Override
        public Set<Map.Entry<String, ScalarValue<?>>> entrySet() {
            HashSet<Map.Entry<String, ScalarValue<?>>> result = new HashSet<>();
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                ScalarValue<?> value = values[i];
                result.add(new SimpleEntry<String, ScalarValue<?>>(column, value));
            }
            return result;
        }

        @Override
        public Set<String> keySet() {
            return Sets.newHashSet(Iterators.forArray(columns));
        }

        @Override
        public Set<ScalarValue<?>> valueSet() {
            return Sets.newHashSet(Iterators.forArray(values));
        }

        @Override
        public ScalarValue get(String key) throws IllegalArgumentException {
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
        public void consume(ForEachConsumer<ScalarValue<?>> consumer) {
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                ScalarValue value = values[i];

                consumer.consume(column, value);
            }
        }

    }

}
