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

package com.torodb.mongowp.mongoserver.api.toro.util;

import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.torod.core.subdocument.ToroDocument;
import javax.annotation.Nonnull;
import org.bson.BSONObject;

/**
 *
 */
public class BSONToroDocument implements ToroDocument {
	
	private final BSONObject document;
	
	public BSONToroDocument(@Nonnull BSONObject document) {
		this.document = document;
	}

	@Override
	public ObjectValue getRoot() {
		return MongoValueConverter.translateObject(document);
	}
	
}
