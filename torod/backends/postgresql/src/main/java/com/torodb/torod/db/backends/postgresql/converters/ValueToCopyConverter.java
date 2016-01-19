
package com.torodb.torod.db.backends.postgresql.converters;

import com.torodb.torod.core.subdocument.values.*;
import com.torodb.torod.db.backends.converters.PatternConverter;
import org.threeten.bp.DateTimeUtils;

/**
 *
 */
public class ValueToCopyConverter implements ValueVisitor<Void, StringBuilder> {
    private static final char[] HEX_CODE = "0123456789ABCDEF".toCharArray();

    private static final char ROW_DELIMETER = '\n';
    private static final char COLUMN_DELIMETER = '\t';
    private static final InArrayConverter IN_ARRAY_CONVERTER = new InArrayConverter();

    public static final ValueToCopyConverter INSTANCE = new ValueToCopyConverter();

    ValueToCopyConverter() {
    }

    @Override
    public Void visit(BooleanValue value, StringBuilder arg) {
        if (value.getValue()) {
            arg.append("true");
        }
        else {
            arg.append("false");
        }
        return null;
    }

    @Override
    public Void visit(NullValue value, StringBuilder arg) {
        arg.append("\\N");
        return null;
    }

    @Override
    public Void visit(ArrayValue value, StringBuilder arg) {
        arg.append('[');
        for (Value<?> child : value) {
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
    public Void visit(IntegerValue value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(LongValue value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(DoubleValue value, StringBuilder arg) {
        arg.append(value.getValue().toString());
        return null;
    }

    @Override
    public Void visit(StringValue value, StringBuilder arg) {
        escape(value.getValue(), arg);
        return null;
    }

    @Override
    public Void visit(TwelveBytesValue value, StringBuilder arg) {
        arg.append("\\x");

        for (byte b : value.getArrayValue()) {
            arg.append(HEX_CODE[(b >> 4) & 0xF]);
            arg.append(HEX_CODE[(b & 0xF)]);
        }

        return null;
    }

    @Override
    public Void visit(DateTimeValue value, StringBuilder arg) {
        arg.append(DateTimeUtils.toSqlTimestamp(value.getValue()).toString());
        return null;
    }

    @Override
    public Void visit(DateValue value, StringBuilder arg) {
        arg.append(DateTimeUtils.toSqlDate(value.getValue()).toString());
        return null;
    }

    @Override
    public Void visit(TimeValue value, StringBuilder arg) {
        arg.append(DateTimeUtils.toSqlTime(value.getValue()).toString());
        return null;
    }

    @Override
    public Void visit(PatternValue value, StringBuilder arg) {
        escape(PatternConverter.toPosixPattern(value), arg);
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
            ValueVisitor<Void, StringBuilder> {

        @Override
        public Void visit(BooleanValue value, StringBuilder arg) {
            if (value.getValue()) {
                arg.append("true");
            } else {
                arg.append("false");
            }
            return null;
        }

        @Override
        public Void visit(NullValue value, StringBuilder arg) {
            arg.append("null");
            return null;
        }

        @Override
        public Void visit(ArrayValue value, StringBuilder arg) {
            arg.append('[');
            for (Value<?> child : value) {
                child.accept(this, arg);
                arg.append(',');
            }
            if (!value.isEmpty()) {
                arg.replace(arg.length() - 1, arg.length(), "]");
            }
            return null;
        }

        @Override
        public Void visit(IntegerValue value, StringBuilder arg) {
            arg.append(value.getValue().toString());
            return null;
        }

        @Override
        public Void visit(LongValue value, StringBuilder arg) {
            arg.append(value.getValue().toString());
            return null;
        }

        @Override
        public Void visit(DoubleValue value, StringBuilder arg) {
            arg.append(value.getValue().toString());
            return null;
        }

        @Override
        public Void visit(StringValue value, StringBuilder arg) {
            arg.append('"');
            escape(value.getValue(), arg);
            arg.append('"');
            return null;
        }

        @Override
        public Void visit(TwelveBytesValue value, StringBuilder arg) {
            arg.append('"');
            arg.append("\\\\x");

            for (byte b : value.getArrayValue()) {
                arg.append(HEX_CODE[(b >> 4) & 0xF]);
                arg.append(HEX_CODE[(b & 0xF)]);
            }

            arg.append('"');
            
            return null;
        }

        @Override
        public Void visit(DateTimeValue value, StringBuilder arg) {
            arg.append('"')
                    .append(DateTimeUtils.toSqlTimestamp(value.getValue()).toString())
                    .append('"');
            return null;
        }

        @Override
        public Void visit(DateValue value, StringBuilder arg) {
            arg.append('"')
                    .append(DateTimeUtils.toSqlDate(value.getValue()).toString())
                    .append('"');
            return null;
        }

        @Override
        public Void visit(TimeValue value, StringBuilder arg) {
            arg.append('"')
                    .append(DateTimeUtils.toSqlTime(value.getValue()).toString())
                    .append('"');
            return null;
        }

        @Override
        public Void visit(PatternValue value, StringBuilder arg) {
            arg.append('"');
            escape(PatternConverter.toPosixPattern(value), arg);
            arg.append('"');
            return null;
        }

    }
}
