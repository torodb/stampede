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


package com.torodb.torod.db.postgresql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class IdsFilter {
    
    public static String escapeSchemaName(String collection) throws IllegalArgumentException {
        filter(collection);
        return collection;
    }
    
    public static String escapeAttributeName(String attributeName) throws IllegalArgumentException {
        filter(attributeName);
        return attributeName;
    }

    public static String escapeIndexName(String indexName) {
        filter(indexName);
        return indexName;
    }
    
    private static void filter(String str) {
        if (str.length() > 63) {
            throw new IllegalArgumentException(str + " is too long to be a "
                    + "valid PostgreSQL name. By default names must be shorter "
                    + "than 64, but it has " + str.length() + " characters");
        }
        Pattern quotesPattern = Pattern.compile("(\"+)");
        Matcher matcher = quotesPattern.matcher(str);
        while (matcher.find()) {
            if (((matcher.end() - matcher.start()) & 1) == 1) { //lenght is uneven
                throw new IllegalArgumentException("The name '" + str + "' is"
                        + "illegal because contains an open quote at " + matcher.start());
            }
        }
    }
    
}
