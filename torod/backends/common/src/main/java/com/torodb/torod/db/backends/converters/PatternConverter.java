
package com.torodb.torod.db.backends.converters;

import java.util.regex.Pattern;

/**
 *
 */
public class PatternConverter {
    
    public static String toPosixPattern(Pattern pattern) {
        int flags = pattern.flags();

        if ((flags & Pattern.UNICODE_CASE) != 0) {
            throw new IllegalArgumentException(
                    "Flag Unix lines is not supported on the database"
            );
        }

        if (pattern.flags() == 0) {
            return pattern.pattern();
        }
        else {
            return toEmbeddedFlags(pattern) + '(' + pattern.pattern() + ')';
        }
    }

    public static Pattern fromPosixPattern(String posixPattern) {
        return Pattern.compile(posixPattern);
    }

    private static String toEmbeddedFlags(Pattern pattern) {
        int flags = pattern.flags();
        if (flags == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder(30);
        sb.append("(?");
        if ((flags & Pattern.CANON_EQ) != 0) {
            throw new IllegalStateException("The flag CANON_EQ cannot be serialized as string");
        }
        if ((flags & Pattern.LITERAL) != 0) {
            throw new IllegalStateException("The flag LITERAL cannot be serialized as string");
        }
        if ((flags & Pattern.CASE_INSENSITIVE) != 0) {
            sb.append('i');
        }
        if ((flags & Pattern.COMMENTS) != 0) {
            sb.append('x');
        }
        if ((flags & Pattern.MULTILINE) != 0) {
            sb.append('m');
        }
        if ((flags & Pattern.DOTALL) != 0) {
            sb.append('s');
        }
        if ((flags & Pattern.UNICODE_CASE) != 0) {
            sb.append('u');
        }
        if ((flags & Pattern.UNIX_LINES) != 0) {
            sb.append('d');
        }
        sb.append(')');

        return sb.toString();
    }

    private PatternConverter() {
    }
}
