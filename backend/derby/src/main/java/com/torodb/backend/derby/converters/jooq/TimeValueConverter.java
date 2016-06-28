package com.torodb.backend.derby.converters.jooq;
import java.sql.Time;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.converters.sql.TimeSqlBinding;
import com.torodb.backend.derby.converters.jooq.binding.TimeBinding;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.TimeType;
import com.torodb.kvdocument.values.KVTime;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;

/**
 *
 */
public class TimeValueConverter implements KVValueConverter<Time, Time, KVTime>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVTime> TYPE = TimeBinding.fromKVValue(KVTime.class, new TimeValueConverter());

    @Override
    public KVType getErasuredType() {
        return TimeType.INSTANCE;
    }

    @Override
    public KVTime from(Time databaseObject) {
        return new LocalTimeKVTime(databaseObject.toLocalTime());
    }

    @Override
    public Time to(KVTime userObject) {
        return Time.valueOf(userObject.getValue());
    }

    @Override
    public Class<Time> fromType() {
        return Time.class;
    }

    @Override
    public Class<KVTime> toType() {
        return KVTime.class;
    }

    @Override
    public SqlBinding<Time> getSqlBinding() {
        return TimeSqlBinding.INSTANCE;
    }
    
}