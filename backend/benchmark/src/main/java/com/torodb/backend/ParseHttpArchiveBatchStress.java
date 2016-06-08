package com.torodb.backend;

import static com.torodb.backend.util.MetaInfoOperation.executeMetaOperation;
import static com.torodb.backend.util.TestDataFactory.COLL1;
import static com.torodb.backend.util.TestDataFactory.DB1;
import static com.torodb.backend.util.TestDataFactory.InitialView;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Stopwatch;
import com.torodb.backend.util.InMemoryRidGenerator;
import com.torodb.backend.util.JsonArchiveFeed;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

/**
 * 
 * To execute code you must download archives from: 
 * https://discuss.httparchive.org/t/how-to-download-the-http-archive-data/679
 * 
 * You can configure the source path, a subset of documents with a stream filter and the batch size
 *
 */
public class ParseHttpArchiveBatchStress {

	public static void main(String[] args) throws IOException {

		MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(InitialView);
		RidGenerator ridGenerator = new InMemoryRidGenerator();

		AtomicLong cont=new AtomicLong(0);
		Stopwatch toroTimer = Stopwatch.createUnstarted();
		JsonArchiveFeed feed = new JsonArchiveFeed("/temp/httparchive/");
		feed.getGroupedFeedForFiles((f)->f.getName().startsWith("160101_1J"), 50).forEach(docStream -> {
			toroTimer.start();
			executeMetaOperation(mvccMetainfoRepository, (mutableSnapshot) -> {
				D2RTranslator translator = new D2RTranslatorImpl(ridGenerator, mutableSnapshot, DB1, COLL1);
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
