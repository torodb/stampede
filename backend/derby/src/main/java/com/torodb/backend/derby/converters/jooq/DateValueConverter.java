package com.torodb.backend.derby.converters.jooq;
import java.sql.Date;
import java.time.LocalDate;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.DateSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.derby.converters.jooq.binding.DateBinding;
import com.torodb.kvdocument.types.DateType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVDate;
import com.torodb.kvdocument.values.heap.LocalDateKVDate;

/**
 *
 */
public class DateValueConverter implements KVValueConverter<Date, KVDate> {
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVDate> TYPE = DateBinding.fromKVValue(KVDate.class, new DateValueConverter());

    @Override
    public KVType getErasuredType() {
        return DateType.INSTANCE;
    }

    @Override
    public KVDate from(Date databaseObject) {
        return new LocalDateKVDate(
                LocalDate.of(databaseObject.getYear() + 1900, databaseObject.getMonth() + 1, databaseObject.getDate())
        );
    }

    @Override
    public Date to(KVDate userObject) {
        LocalDate date = userObject.getValue();
        return new Date(date.getYear() - 1900, date.getMonthValue() -1, date.getDayOfMonth());
    }

    @Override
    public Class<Date> fromType() {
        return Date.class;
    }

    @Override
    public Class<KVDate> toType() {
        return KVDate.class;
    }

    @Override
    public SqlBinding<Date> getSqlBinding() {
        return DateSqlBinding.INSTANCE;
    }

}
