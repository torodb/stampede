package com.torodb.torod.db.backends.converters.jooq;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Objects;

import javax.json.JsonValue;

import org.jooq.Binding;
import org.jooq.BindingGetResultSetContext;
import org.jooq.BindingGetSQLInputContext;
import org.jooq.BindingGetStatementContext;
import org.jooq.BindingRegisterContext;
import org.jooq.BindingSQLContext;
import org.jooq.BindingSetSQLOutputContext;
import org.jooq.BindingSetStatementContext;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;

import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.db.backends.converters.array.ArrayConverter;

public class JSONBBinding<T> implements Binding<String, T> {

    private static final long serialVersionUID = 1L;

    public static <UT extends ScalarValue<?>> DataTypeForScalar<UT> fromScalarValue(Class<UT> type, SubdocValueConverter<String, UT> converter) {
        return DataTypeForScalar.from(new DefaultDataType<>(null, String.class, "jsonb"), converter, new JSONBBinding<>(converter));
    }
    
    public static <UT extends ScalarValue<?>, V extends JsonValue> DataType<UT> fromScalarValue(final Class<UT> type, final ArrayConverter<V, UT> arrayConverter) {
        Converter<String, UT> converter = new ArrayToJooqConverter<>(type, arrayConverter);
        return new DefaultDataType<>(null, String.class, "jsonb").asConvertedDataType(new JSONBBinding<>(converter));
    }
    
    public static <UT> DataType<UT> fromType(Class<UT> type, Converter<String, UT> converter) {
        return new DefaultDataType<>(null, String.class, "jsonb").asConvertedDataType(new JSONBBinding<>(converter));
    }

    private final Converter<String, T> converter;
    
    public JSONBBinding(Converter<String, T> converter) {
        super();
        this.converter = converter;
    }

    @Override
    public Converter<String, T> converter() {
        return converter;
    }

    @Override
    public void sql(BindingSQLContext<T> ctx) throws SQLException {
        ctx.render().visit(DSL.val(ctx.convert(converter()).value())).sql("::jsonb");
    }

    @Override
    public void register(BindingRegisterContext<T> ctx) throws SQLException {
        ctx.statement().registerOutParameter(ctx.index(), Types.VARCHAR);
    }

    @Override
    public void set(BindingSetStatementContext<T> ctx) throws SQLException {
        ctx.statement().setString(ctx.index(), Objects.toString(ctx.convert(converter()).value(), null));
    }

    @Override
    public void get(BindingGetResultSetContext<T> ctx) throws SQLException {
        ctx.convert(converter()).value(ctx.resultSet().getString(ctx.index()));
    }

    @Override
    public void get(BindingGetStatementContext<T> ctx) throws SQLException {
        ctx.convert(converter()).value(ctx.statement().getString(ctx.index()));
    }

    @Override
    public void set(BindingSetSQLOutputContext<T> ctx) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void get(BindingGetSQLInputContext<T> ctx) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
}
