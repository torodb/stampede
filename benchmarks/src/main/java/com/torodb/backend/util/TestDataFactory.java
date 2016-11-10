/*
 * MongoWP - ToroDB-poc: Backends benchmark
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.util;

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.heap.LongKVInstant;

public class TestDataFactory {

	public static final String DB1 = "test1";
	public static final String COLL1 = "coll1Test1";
	public static final String COLL2 = "coll2Test1";

	public static final String DB2 = "test2";
	public static final String COLL3 = "coll1Test2";
	public static final String COLL4 = "coll2Test2";

	public static final ImmutableMetaSnapshot initialView = new ImmutableMetaSnapshot.Builder()
			.put(new ImmutableMetaDatabase.Builder(DB1, DB1)
					.put(new ImmutableMetaCollection.Builder(COLL1, COLL1).build())
					.put(new ImmutableMetaCollection.Builder(COLL2, COLL2).build()).build())
			.put(new ImmutableMetaDatabase.Builder(DB2, DB2)
					.put(new ImmutableMetaCollection.Builder(COLL3, COLL3).build())
					.put(new ImmutableMetaCollection.Builder(COLL4, COLL4).build()).build())
			.build();
	
	//	{
	//	"_id" : {"$oid":"5298a5a03b3f4220588fe57c"},
	//    "created_on" : {"$date": 1299715200000},
	//    "value" : 0.1647851116706831,
	//    "by" : "john",
	//    "to" : "peter",
	//    "from" : "smith",
	//    "subdoc" : {
	//    	"name" : "lopez"
	//    }
	//    "subArr" : [
	//			1,2,3,4
	//    ]
	//}		
	
	public static KVDocument buildDoc(){
		HashDocumentBuilder b=new HashDocumentBuilder();
		return b.appendMongoId("_id", "5298a5a03b3f4220588fe57c")
			.append("created_on",new LongKVInstant(1299715200000L))
			.append("value", 0.1647851116706831)
			.append("by","john")
			.append("to","peter")
			.append("from","smith")
			.append("subdoc",new HashDocumentBuilder().append("name","lopez").build())
			.appendArr("subArr",KVInteger.of(1),KVInteger.of(2),KVInteger.of(3),KVInteger.of(4))
		.build();
	}
}
