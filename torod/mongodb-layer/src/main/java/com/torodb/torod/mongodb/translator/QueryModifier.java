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

package com.torodb.torod.mongodb.translator;

/**
 *
 */
public enum QueryModifier {
	COMMENT("$comment"), // Adds a comment to the query to identify queries in the database profiler output.
	EXPLAIN("$explain"), // Forces MongoDB to report on query execution plans. See explain().
	HINT("$hint"), // Forces MongoDB to use a specific index. See hint()
	MAX_SCAN("$maxScan"), // Limits the number of documents scanned.
	MAX_TIME_MS("$maxTimeMS"), // Specifies a cumulative time limit in milliseconds for processing operations on a cursor. See maxTimeMS().
	MAX("$max"), // Specifies an exclusive upper limit for the index to use in a query. See max().
	MIN("$min"), // Specifies an inclusive lower limit for the index to use in a query. See min().
	ORDER_BY("$orderby", "orderby"), // Returns a cursor with documents sorted according to a sort specification. See sort().
	RETURN_KEY("$returnKey"), // Forces the cursor to only return fields included in the index.
	SHOW_DISK_LOC("$showDiskLoc"), // Modifies the documents returned to include references to the on-disk location of each document.
	SNAPSHOT("$snapshot"); // Forces the query to use the index on the _id field. See snapshot().

	public static QueryModifier getByKey(String key) {
		for (QueryModifier modifier : QueryModifier.values()) {
			for (String modifierKey : modifier.keys) {
				if (modifierKey.equals(key)) {
					return modifier;
				}
			}
		}

		return null;
	}

	private final String[] keys;

	private QueryModifier(String... keys) {
		this.keys = keys;
	}
}
