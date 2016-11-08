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
package com.torodb.backend.d2r;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.torodb.backend.util.InMemoryRidGenerator;
import com.torodb.backend.util.JsonArchiveFeed;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.core.d2r.DefaultIdentifierFactory;
import com.torodb.d2r.MockIdentifierInterface;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.torodb.backend.util.MetaInfoOperation.executeMetaOperation;
import static com.torodb.backend.util.TestDataFactory.*;

import com.torodb.core.d2r.ReservedIdGenerator;

/**
 * 
 * To execute code you must download archives from: 
 * https://www.githubarchive.org/
 * 
 * You can configure the source path, a subset of documents with a stream filter and the batch size
 *
 */
public class ParseGitHubBatchStackStress {

	public static void main(String[] args) throws IOException {

		MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(initialView);
	    TableRefFactory tableRefFactory = new TableRefFactoryImpl();
		ReservedIdGenerator ridGenerator = new InMemoryRidGenerator(new ThreadFactoryBuilder().build());
		IdentifierFactory identifierFactory = new DefaultIdentifierFactory(new MockIdentifierInterface());

		AtomicLong cont=new AtomicLong(0);
		Stopwatch toroTimer = Stopwatch.createUnstarted();
		JsonArchiveFeed feed = new JsonArchiveFeed("/temp/archive/");
		feed.getGroupedFeedForLines(line -> line.length() < 1024, 50).forEach(docStream -> {
			toroTimer.start();
			executeMetaOperation(mvccMetainfoRepository, (mutableSnapshot) -> {
                MutableMetaDatabase db = mutableSnapshot.getMetaDatabaseByName(DB1);
				D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, db, db.getMetaCollectionByName(COLL1));
				docStream.forEach(doc -> {
					translator.translate(doc);	
				});
				for (DocPartData table : translator.getCollectionDataAccumulator().orderedDocPartData()) {
					cont.addAndGet(table.rowCount());
				}
			});
			toroTimer.stop();
		});

		double tt = (double) toroTimer.elapsed(TimeUnit.MICROSECONDS);

		System.out.println("Readed: " + feed.datasize / (1024 * 1024) + " MBytes");
		System.out.println("Documents: " + feed.documents);
		System.out.println("Rows:  " + cont);
		System.out.println("Time Toro:   " + tt + " microsecs");
		System.out.println("Speed: " + (tt / feed.documents) + " microsecs per document");
		System.out.println("DPS: " + ((feed.documents / tt) * 1000000) + " documents per second");
	}

}
