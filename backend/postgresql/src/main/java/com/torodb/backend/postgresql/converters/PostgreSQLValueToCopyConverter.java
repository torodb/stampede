
package com.torodb.backend.postgresql.converters;

import com.torodb.backend.postgresql.converters.util.CopyEscaper;
import com.torodb.common.util.HexUtils;
import com.torodb.common.util.TextEscaper;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVBinary;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDate;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInstant;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVMongoObjectId;
import com.torodb.kvdocument.values.KVMongoTimestamp;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVString;
import com.torodb.kvdocument.values.KVTime;
import com.torodb.kvdocument.values.KVValueVisitor;

/**
 *
 */
public class PostgreSQLValueToCopyConverter implements KVValueVisitor<Void, StringBuilder> {
    public static final PostgreSQLValueToCopyConverter INSTANCE = new PostgreSQLValueToCopyConverter();
    
    private static final TextEscaper ESCAPER = CopyEscaper.INSTANCE;

    PostgreSQLValueToCopyConverter() {
    }

    @Override
    public Void visit(KVBoolean value, StringBuilder arg) {
        if (value.getValue()) {
            arg.append("true");
        }
        else {
            arg.append("false");
        }
        return null;
    }

    @Override
    public Void visit(KVNull value, StringBuilder arg) {
        arg.append("true");
        return null;
    }

    @Override
    public Void visit(KVArray value, StringBuilder arg) {
        throw new UnsupportedOperationException("Ouch this should not occur");
    }

    @Override
    public Void visit(KVInteger value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(KVLong value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(KVDouble value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(KVString value, StringBuilder arg) {
        ESCAPER.appendEscaped(arg, value.getValue());
        return null;
    }

    @Override
    public Void visit(KVMongoObjectId value, StringBuilder arg) {
        arg.append("\\\\x");

        HexUtils.bytes2Hex(value.getArrayValue(), arg);

        return null;
    }

    @Override
    public Void visit(KVBinary value, StringBuilder arg) {
        arg.append("\\\\x");

        HexUtils.bytes2Hex(value.getByteSource().read(), arg);

        return null;
    }

    @Override
    public Void visit(KVInstant value, StringBuilder arg) {
        arg.append('\'')
                //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                .append(value.getValue().toString())
                .append('\'');
        return null;
    }

    @Override
    public Void visit(KVDate value, StringBuilder arg) {
        arg.append('\'')
                //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                .append(value.getValue().toString())
                .append('\'');
        return null;
    }

    @Override
    public Void visit(KVTime value, StringBuilder arg) {
        arg.append('\'')
                //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                .append(value.getValue().toString())
                .append('\'');
        return null;
    }

    @Override
    public Void visit(KVMongoTimestamp value, StringBuilder arg) {
        arg.append('(').append(value.getSecondsSinceEpoch()).append(',').append(value.getOrdinal()).append(')');
        return null;
    }

    @Override
    public Void visit(KVDocument value, StringBuilder arg) {
        throw new UnsupportedOperationException("Ouch this should not occur");
    }
}
