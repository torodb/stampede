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

import com.eightkdata.nettybson.api.BSONDocument;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import org.bson.BSONObject;

/**
 *
 */
public class BSONDocuments implements Iterable<BSONDocument> {
	
	private final Iterable<?> documents;
	private int size = 0;	
	
	public BSONDocuments(@Nonnull Iterable<?> documents) {
		this.documents = documents;
		
		size = 0;
		Iterator<?> iterator = this.documents.iterator();
		while (iterator.hasNext()) {
			Object object = iterator.next();
			assert object instanceof BSONObject;
			size++;
		}
	}
	
	public int size() {
		return size;
	}
	
	@Override
	public Iterator<BSONDocument> iterator() {
		return new Iterator<BSONDocument>() {
			Iterator<?> iterator = documents.iterator();
			
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public BSONDocument next() {
				ToroDocument document = (ToroDocument) iterator.next();
				Map<String, Object> keyValues = new HashMap<String, Object>();
				BSONObject object = MongoValueConverter.translateObject(document.getRoot());
				for (String key : object.keySet()) {
					keyValues.put(key, object.get(key));
				}

				return new MongoBSONDocument(keyValues);
			}

			@Override
			public void remove() {
				iterator.remove();
				size--;
			}
		};
	}
	
}
