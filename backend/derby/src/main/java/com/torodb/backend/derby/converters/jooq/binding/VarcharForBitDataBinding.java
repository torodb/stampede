package com.torodb.backend.derby.converters.jooq.binding;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

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
import org.jooq.util.derby.DerbyDataType;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.kvdocument.values.KVValue;

public class VarcharForBitDataBinding<T> implements Binding<byte[], T> {

    private static final long serialVersionUID = 1L;

    public static <UT extends KVValue<?>> DataTypeForKV<UT> fromKVValue(Class<UT> type, KVValueConverter<byte[], UT> converter, int size) {
        return DataTypeForKV.from(new DefaultDataType<byte[]>(null, byte[].class, "varchar (" + size + ") for bit data"), converter, new VarcharForBitDataBinding<UT>(converter), Types.VARBINARY);
    }
    
    public static <UT> DataType<UT> fromType(Class<UT> type, Converter<byte[], UT> converter, int size) {
        return new DefaultDataType<byte[]>(null, byte[].class, "varchar (" + size + ") for bit data").asConvertedDataType(new VarcharForBitDataBinding<UT>(converter));
    }

    private final Converter<byte[], T> converter;
    
    public VarcharForBitDataBinding(Converter<byte[], T> converter) {
        super();
        this.converter = converter;
    }

    @Override
    public Converter<byte[], T> converter() {
        return converter;
    }

    @Override
    public void sql(BindingSQLContext<T> ctx) throws SQLException {
        ctx.render().visit(DSL.val(ctx.convert(converter()).value(), DerbyDataType.VARCHAR));
    }

    @Override
    public void register(BindingRegisterContext<T> ctx) throws SQLException {
        ctx.statement().registerOutParameter(ctx.index(), Types.DATE);
    }

    @Override
    public void set(BindingSetStatementContext<T> ctx) throws SQLException {
        byte[] value = ctx.convert(converter()).value();
        if (value != null) {
            ctx.statement().setBytes(ctx.index(), value);
        } else {
            ctx.statement().setNull(ctx.index(), Types.VARBINARY);
        }
    }

    @Override
    public void get(BindingGetResultSetContext<T> ctx) throws SQLException {
        ctx.convert(converter()).value(ctx.resultSet().getBytes(ctx.index()));
    }

    @Override
    public void get(BindingGetStatementContext<T> ctx) throws SQLException {
        ctx.convert(converter()).value(ctx.statement().getBytes(ctx.index()));
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
