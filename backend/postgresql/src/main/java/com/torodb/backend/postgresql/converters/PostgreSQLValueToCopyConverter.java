
package com.torodb.backend.postgresql.converters;

import com.torodb.backend.postgresql.converters.sql.StringSqlBinding;
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
    
    private static final TextEscaper ESCAPER = TextEscaper.create(StringSqlBinding.Escapable.values(), CopyEscapable.values());

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
    
    private static final char COPY_ESCAPE_CHARACTER = '\\';
    private static final char ROW_DELIMETER_CHARACTER = '\n';
    private static final char COLUMN_DELIMETER_CHARACTER = '\t';
    private static final char CARRIAGE_RETURN_CHARACTER = '\r';
    
    private enum CopyEscapable implements TextEscaper.Escapable {
        ROW_DELIMETER(ROW_DELIMETER_CHARACTER, ROW_DELIMETER_CHARACTER),
        COLUMN_DELIMETER(COLUMN_DELIMETER_CHARACTER, COLUMN_DELIMETER_CHARACTER),
        CARRIAGE_RETURN(CARRIAGE_RETURN_CHARACTER, CARRIAGE_RETURN_CHARACTER),
        COPY_ESCAPE(COPY_ESCAPE_CHARACTER, COPY_ESCAPE_CHARACTER);
        
        private final char character;
        private final char suffixCharacter;
        
        private CopyEscapable(char character, char suffixCharacter) {
            this.character = character;
            this.suffixCharacter = suffixCharacter;
        }

        @Override
        public char getCharacter() {
            return character;
        }

        @Override
        public char getSuffixCharacter() {
            return suffixCharacter;
        }

        @Override
        public char getEscapeCharacter() {
            return COPY_ESCAPE_CHARACTER;
        }
    }
}
