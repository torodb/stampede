package com.torodb.backend.util;

import com.torodb.core.transaction.metainf.MetainfoRepository.MergerStage;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;
import java.util.function.Consumer;

public class MetaInfoOperation {

	public static void executeMetaOperation(MvccMetainfoRepository mvcc, Consumer<MutableMetaSnapshot> consumer){
		MutableMetaSnapshot mutableSnapshot;
		try (SnapshotStage snapshot = mvcc.startSnapshotStage()) {
			mutableSnapshot = snapshot.createMutableSnapshot();
		}
		
		consumer.accept(mutableSnapshot);
		
		try (MergerStage mergeStage = mvcc.startMerge(mutableSnapshot)) {
            mergeStage.commit();
        } catch (UnmergeableException ex) {
            throw new AssertionError("Unmergeable changes", ex);
        }
	}

}
