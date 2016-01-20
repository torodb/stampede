/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.backends.tables;

import com.torodb.torod.db.backends.DatabaseInterface;
import javax.annotation.Nonnull;
import javax.inject.Inject;

/**
 *
 */
public class SubDocHelper {

    private static final char ESCAPE_CHARACTER = '_';
    private final DatabaseInterface databaseInterface;

    @Inject
    public SubDocHelper(@Nonnull DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    public String toColumnName(String attName) {
        if (needsToByEscaped(attName)) {
            return ESCAPE_CHARACTER + attName;
        }

        return databaseInterface.escapeAttributeName(attName);
    }

    public static String toAttributeName(String fieldName) {
        if (isEscaped(fieldName)) {
            return fieldName.substring(1);
        }

        return fieldName;
    }

    private static boolean isEscaped(String fieldName) {
        int i = getFirstRelevantCharIndex(fieldName);
        if (i == -1) {
            return false;
        }
        if (i == 0) {
            return false;
        }
        return isDid(fieldName, i) || isIndex(fieldName, i);
    }

    private boolean needsToByEscaped(String attName) {
        int i = getFirstRelevantCharIndex(attName);
        if (i == -1) {
            i = 0;
        }
        assert attName.charAt(i) != ESCAPE_CHARACTER;

        return isDid(attName, i) || isIndex(attName, i);
    }

    /**
     * @param str
     * @return the index on str of the first char that is not an escape
     *         character or -1 if str only contains escape characters
     */
    private static int getFirstRelevantCharIndex(String str) {
        final int attLength = str.length();

        int i;
        for (i = 0; i < attLength; i++) {
            if (str.charAt(i) != ESCAPE_CHARACTER) {
                break;
            }
        }
        if (i >= attLength) {
            return -1;
        }
        assert str.charAt(i) != ESCAPE_CHARACTER;
        return i;
    }

    private static boolean isDid(String attName, int i) {
        return attName.length() == i + 2 + 1
                && attName.charAt(i) == 'd'
                && attName.charAt(i+1) == 'i'
                && attName.charAt(i+2) == 'd';
    }

    private static boolean isIndex(String attName, int i) {
        return attName.length() == i + 4 + 1
                && attName.charAt(i) == 'i'
                && attName.charAt(i+1) == 'n'
                && attName.charAt(i+2) == 'd'
                && attName.charAt(i+3) == 'e'
                && attName.charAt(i+4) == 'x';
    }

}
