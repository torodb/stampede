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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class SubDocHelper {

    private static final Pattern LIKE_DID_PATTERN = Pattern.compile("_*" + SubDocTable.DID_COLUMN_NAME);
    private static final Pattern LIKE_INDEX_PATTERN = Pattern.compile("_*" + SubDocTable.INDEX_COLUMN_NAME);

    private final DatabaseInterface databaseInterface;

    @Inject
    public SubDocHelper(@Nonnull DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    public String toColumnName(String attName) {
        Matcher matcher = LIKE_DID_PATTERN.matcher(attName);
        if (matcher.find()) {
            return "_" + attName;
        }
        matcher = LIKE_INDEX_PATTERN.matcher(attName);
        if (matcher.find()) {
            return "_" + attName;
        }

        return databaseInterface.escapeAttributeName(attName);
    }

    public static String toAttributeName(String fieldName) {
        Matcher matcher = LIKE_DID_PATTERN.matcher(fieldName);
        if (matcher.find()) {
            return fieldName.substring(1);
        }
        matcher = LIKE_INDEX_PATTERN.matcher(fieldName);
        if (matcher.find()) {
            return fieldName.substring(1);
        }

        return fieldName;
    }

}
