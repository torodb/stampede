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

/**
 *
 */
public class IdsFilter {
    
    public static void filterCollectionName(String collection) throws IllegalArgumentException {
        filter(collection);
    }
    
    public static String escapeAttributeName(String attributeName) throws IllegalArgumentException {
//        return new StringBuilder(attributeName.length() + 2)
//                .append('"')
//                .append(attributeName)
//                .append('"')
//                .toString();
        return attributeName;
    }
    
    private static void filter(String str) {
        for (char c : str.toCharArray()) {
            if (!Character.isLowerCase(c) && !Character.isDigit(c) && c != '_') {
                throw new IllegalArgumentException("At the present time Torod doesn't support '" + c + "' as "
                        + "identifier character. Only a alphanumeric letters and '_' are supported");
            }
        }
    }
    
}
