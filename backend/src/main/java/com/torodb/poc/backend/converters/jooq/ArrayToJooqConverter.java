package com.torodb.poc.backend.converters.jooq;

import javax.json.JsonValue;

import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.impl.DefaultDataType;

import com.torodb.kvdocument.values.KVValue;
import com.torodb.poc.backend.converters.array.ArrayConverter;

public class ArrayToJooqConverter<UT extends KVValue<?>> implements Converter<String, UT> {
    
    private static final long serialVersionUID = 1L;

    public static <UT extends KVValue<?>, V extends JsonValue> DataType<UT> fromScalarValue(final Class<UT> type, final ArrayConverter<V, UT> arrayConverter, String typeName) {
        Converter<String, UT> converter = new ArrayToJooqConverter<>(type, arrayConverter);
        return new DefaultDataType<String>(null, String.class, typeName).asConvertedDataType(converter);
    }
    
    private final Class<UT> type;
    private final ArrayConverter<?, UT> arrayConverter;
    
    public ArrayToJooqConverter(Class<UT> type, ArrayConverter<?, UT> arrayConverter) {
        super();
        this.type = type;
        this.arrayConverter = arrayConverter;
    }
    public UT from(String databaseObject) {
        throw new RuntimeException("This conversor should not be used to convert from a database object");
    }
    public String to(UT userObject) {
        return arrayConverter.toJsonLiteral(userObject);
    }
    public Class<String> fromType() {
        return String.class;
    }
    public Class<UT> toType() {
        return type;
    }
}
