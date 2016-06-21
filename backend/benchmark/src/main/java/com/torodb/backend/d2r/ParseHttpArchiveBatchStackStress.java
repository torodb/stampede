package com.torodb.backend.d2r;

import com.google.common.base.Stopwatch;
import com.torodb.backend.util.InMemoryRidGenerator;
import com.torodb.backend.util.JsonArchiveFeed;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockIdentifierInterface;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.torodb.backend.util.MetaInfoOperation.executeMetaOperation;
import static com.torodb.backend.util.TestDataFactory.*;

/**
 * 
 * To execute code you must download archives from: 
 * https://discuss.httparchive.org/t/how-to-download-the-http-archive-data/679
 * 
 * You can configure the source path, a subset of documents with a stream filter and the batch size
 *
 */

public class ParseHttpArchiveBatchStackStress {

	public static void main(String[] args) throws IOException {

		MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(InitialView);
	    TableRefFactory tableRefFactory = new TableRefFactoryImpl();
		RidGenerator ridGenerator = new InMemoryRidGenerator();
		IdentifierFactory identifierFactory = new IdentifierFactoryImpl(new MockIdentifierInterface());

		AtomicLong cont=new AtomicLong(0);
		Stopwatch toroTimer = Stopwatch.createUnstarted();
		JsonArchiveFeed feed = new JsonArchiveFeed("/temp/httparchive/");
		feed.getGroupedFeedForFiles((f)->f.getName().startsWith("160101_1J"), 50).forEach(docStream -> {
			toroTimer.start();
			executeMetaOperation(mvccMetainfoRepository, (mutableSnapshot) -> {
                MutableMetaDatabase db = mutableSnapshot.getMetaDatabaseByName(DB1);
				D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, db, db.getMetaCollectionByName(COLL1));
				docStream.forEach(doc -> {
					translator.translate(doc);	
				});
				for (DocPartData table : translator.getCollectionDataAccumulator()) {
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
