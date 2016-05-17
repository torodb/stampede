
package com.torodb.torod.db.backends.greenplum.converters;

import com.torodb.common.util.HexUtils;
import com.torodb.common.util.OctetUtils;
import com.torodb.torod.core.subdocument.values.ScalarArray;
import com.torodb.torod.core.subdocument.values.ScalarBinary;
import com.torodb.torod.core.subdocument.values.ScalarBoolean;
import com.torodb.torod.core.subdocument.values.ScalarDate;
import com.torodb.torod.core.subdocument.values.ScalarDouble;
import com.torodb.torod.core.subdocument.values.ScalarInstant;
import com.torodb.torod.core.subdocument.values.ScalarInteger;
import com.torodb.torod.core.subdocument.values.ScalarLong;
import com.torodb.torod.core.subdocument.values.ScalarMongoObjectId;
import com.torodb.torod.core.subdocument.values.ScalarMongoTimestamp;
import com.torodb.torod.core.subdocument.values.ScalarNull;
import com.torodb.torod.core.subdocument.values.ScalarString;
import com.torodb.torod.core.subdocument.values.ScalarTime;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.core.subdocument.values.ScalarValueVisitor;

/**
 *
 */
public class GreenplumValueToCopyConverter implements ScalarValueVisitor<Void, StringBuilder> {
    private static final char ROW_DELIMETER = '\n';
    private static final char COLUMN_DELIMETER = '\t';
    private static final InArrayConverter IN_ARRAY_CONVERTER = new InArrayConverter();

    public static final GreenplumValueToCopyConverter INSTANCE = new GreenplumValueToCopyConverter();

    GreenplumValueToCopyConverter() {
    }

    @Override
    public Void visit(ScalarBoolean value, StringBuilder arg) {
        if (value.getValue()) {
            arg.append("true");
        }
        else {
            arg.append("false");
        }
        return null;
    }

    @Override
    public Void visit(ScalarNull value, StringBuilder arg) {
        arg.append("\\N");
        return null;
    }

    @Override
    public Void visit(ScalarArray value, StringBuilder arg) {
        arg.append('[');
        for (ScalarValue<?> child : value) {
            child.accept(IN_ARRAY_CONVERTER, arg);
            arg.append(',');
        }
        if (!value.isEmpty()) {
            arg.replace(arg.length() - 1, arg.length(), "]");
        }
        else {
            arg.append(']');
        }
        return null;
    }

    @Override
    public Void visit(ScalarInteger value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(ScalarLong value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(ScalarDouble value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(ScalarString value, StringBuilder arg) {
        escape(value.getValue(), arg);
        return null;
    }

    @Override
    public Void visit(ScalarMongoObjectId value, StringBuilder arg) {
        OctetUtils.bytes2Octet(value.getArrayValue(), arg);

        return null;
    }

    @Override
    public Void visit(ScalarBinary value, StringBuilder arg) {
        OctetUtils.bytes2Octet(value.getByteSource().read(), arg);

        return null;
    }

    @Override
    public Void visit(ScalarInstant value, StringBuilder arg) {
        arg.append('\'')
                //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                .append(value.getValue().toString())
                .append('\'');
        return null;
    }

    @Override
    public Void visit(ScalarDate value, StringBuilder arg) {
        arg.append('\'')
                //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                .append(value.getValue().toString())
                .append('\'');
        return null;
    }

    @Override
    public Void visit(ScalarTime value, StringBuilder arg) {
        arg.append('\'')
                //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                .append(value.getValue().toString())
                .append('\'');
        return null;
    }

    @Override
    public Void visit(ScalarMongoTimestamp value, StringBuilder arg) {
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

    static class InArrayConverter implements
            ScalarValueVisitor<Void, StringBuilder> {

        @Override
        public Void visit(ScalarBoolean value, StringBuilder arg) {
            if (value.getValue()) {
                arg.append("true");
            } else {
                arg.append("false");
            }
            return null;
        }

        @Override
        public Void visit(ScalarNull value, StringBuilder arg) {
            arg.append("null");
            return null;
        }

        @Override
        public Void visit(ScalarArray value, StringBuilder arg) {
            arg.append('[');
            for (ScalarValue<?> child : value) {
                child.accept(this, arg);
                arg.append(',');
            }
            if (!value.isEmpty()) {
                arg.replace(arg.length() - 1, arg.length(), "]");
            }
            return null;
        }

        @Override
        public Void visit(ScalarInteger value, StringBuilder arg) {
            arg.append(value.getValue().toString());
            return null;
        }

        @Override
        public Void visit(ScalarLong value, StringBuilder arg) {
            arg.append(value.getValue().toString());
            return null;
        }

        @Override
        public Void visit(ScalarDouble value, StringBuilder arg) {
            arg.append(value.getValue().toString());
            return null;
        }

        @Override
        public Void visit(ScalarString value, StringBuilder arg) {
            arg.append('"');
            escape(value.getValue(), arg);
            arg.append('"');
            return null;
        }

        @Override
        public Void visit(ScalarMongoObjectId value, StringBuilder arg) {
            arg.append('"');
            arg.append("\\\\x");

            HexUtils.bytes2Hex(value.getArrayValue(), arg);

            arg.append('"');
            
            return null;
        }

        @Override
        public Void visit(ScalarBinary value, StringBuilder arg) {
            arg.append('"');
            arg.append("\\\\x");

            HexUtils.bytes2Hex(value.getByteSource().read(), arg);

            arg.append('"');

            return null;
        }

        @Override
        public Void visit(ScalarInstant value, StringBuilder arg) {
            arg.append('"')
                    //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                    .append(value.getValue().toString())
                    .append('"');
            return null;
        }

        @Override
        public Void visit(ScalarDate value, StringBuilder arg) {
            arg.append('"')
                    //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                    .append(value.getValue().toString())
                    .append('"');
            return null;
        }

        @Override
        public Void visit(ScalarTime value, StringBuilder arg) {
            arg.append('"')
                    //this prints the value on ISO-8601, which is the recommended format on PostgreSQL
                    .append(value.getValue().toString())
                    .append('"');
            return null;
        }

        @Override
        public Void visit(ScalarMongoTimestamp value, StringBuilder arg) {
            arg.append("{\"secs\":").append(value.getSecondsSinceEpoch())
                    .append(",\"counter\":").append(value.getOrdinal())
                    .append('}');

            return null;
        }
    }
}
