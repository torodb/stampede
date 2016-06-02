
package com.torodb.backend.postgresql.converters;

import com.torodb.backend.mocks.ToroImplementationException;
import com.torodb.common.util.HexUtils;
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
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.KVValueVisitor;

/**
 *
 */
public class PostgreSQLValueToCopyConverter implements KVValueVisitor<Void, StringBuilder> {
    private static final char ROW_DELIMETER = '\n';
    private static final char COLUMN_DELIMETER = '\t';

    public static final PostgreSQLValueToCopyConverter INSTANCE = new PostgreSQLValueToCopyConverter();

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
        arg.append("\\N");
        return null;
    }

    @Override
    public Void visit(KVArray value, StringBuilder arg) {
        throw new ToroImplementationException("Ouch this should not occur");
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
        escape(value.getValue(), arg);
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

    private static void escape(String nonEscaped, StringBuilder appender) {
        if (!needsEscape(nonEscaped)) { //doing that we reduce the number of created objects
            appender.append(nonEscaped);
            return ;
        }
        
        appender.ensureCapacity(nonEscaped.length() + 16);
        int lenght = nonEscaped.length();
        int i = 0;
        while (i < lenght) {
            char c = nonEscaped.charAt(i);
            switch (c) {
                case ROW_DELIMETER:
                case COLUMN_DELIMETER:
                case '\\': //this can include false positives
                case '\r':
                    appender.append('\\');
                    break;
                default:
            }
            appender.append(c);
            i++;
        }
    }

    private static boolean needsEscape(String nonEscaped) {
        int lenght = nonEscaped.length();
        for (int i = 0; i < lenght; i++) {
            switch (nonEscaped.charAt(i)) {
                case ROW_DELIMETER:
                case COLUMN_DELIMETER:
                case '\\': //this can include false positives
                case '\r':
                    return true;
            }
        }
        return false;
    }

    @Override
    public Void visit(KVDocument value, StringBuilder arg) {
        throw new ToroImplementationException("Ouch this should not occur");
    }
}
