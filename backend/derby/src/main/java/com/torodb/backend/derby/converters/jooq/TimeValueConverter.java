package com.torodb.backend.derby.converters.jooq;
import java.sql.Time;
import java.time.LocalTime;

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
public class TimeValueConverter implements KVValueConverter<Time, KVTime>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVTime> TYPE = TimeBinding.fromKVValue(KVTime.class, new TimeValueConverter());

    @Override
    public KVType getErasuredType() {
        return TimeType.INSTANCE;
    }

    @SuppressWarnings("deprecation")
    @Override
    public KVTime from(Time databaseObject) {
        return new LocalTimeKVTime(
                LocalTime.of(databaseObject.getHours(), databaseObject.getMinutes(), databaseObject.getSeconds())
        );
    }

    @SuppressWarnings("deprecation")
    @Override
    public Time to(KVTime userObject) {
        LocalTime time = userObject.getValue();
        return new Time(
                time.getHour(), time.getMinute(), time.getSecond());
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