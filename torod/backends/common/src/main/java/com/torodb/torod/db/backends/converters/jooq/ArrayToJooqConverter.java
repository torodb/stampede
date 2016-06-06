package com.torodb.torod.db.backends.converters.jooq;

import javax.json.JsonValue;

import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.impl.DefaultDataType;

import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.db.backends.converters.array.ArrayConverter;

public class ArrayToJooqConverter<UT extends ScalarValue<?>> implements Converter<String, UT> {
    
    private static final long serialVersionUID = 1L;

    public static <UT extends ScalarValue<?>, V extends JsonValue> DataType<UT> fromScalarValue(final Class<UT> type, final ArrayConverter<V, UT> arrayConverter, String typeName) {
        Converter<String, UT> converter = new ArrayToJooqConverter<>(type, arrayConverter);
        return new DefaultDataType<>(null, String.class, typeName).asConvertedDataType(converter);
    }
    
    private final Class<UT> type;
    private final ArrayConverter<?, UT> arrayConverter;
    
    public ArrayToJooqConverter(Class<UT> type, ArrayConverter<?, UT> arrayConverter) {
        super();
        this.type = type;
        this.arrayConverter = arrayConverter;
    }
    public UT from(String databaseObject) {
        throw new ToroRuntimeException("This conversor should not be used to convert from a database object");
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
