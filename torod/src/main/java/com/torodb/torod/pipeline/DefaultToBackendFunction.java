package com.torodb.torod.pipeline;

import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.BackendConnectionJob;
import com.torodb.core.dsl.backend.BackendConnectionJobFactory;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import java.util.ArrayList;
import java.util.function.Function;

/**
 *
 */
public class DefaultToBackendFunction implements
        Function<CollectionData, Iterable<BackendConnectionJob>> {

    private final BackendConnectionJobFactory factory;
    private final MetaDatabase database;
    private final MetaCollection collection;

    public DefaultToBackendFunction(BackendConnectionJobFactory factory, MetaDatabase database, MetaCollection collection) {
        this.factory = factory;
        this.database = database;
        this.collection = collection;
    }

    public Iterable<BackendConnectionJob> apply(CollectionData collectionData) {
        ArrayList<BackendConnectionJob> jobs = new ArrayList<>();
        for (DocPartData docPartData : collectionData) {
            assert docPartData.getMetaDocPart() instanceof BatchMetaDocPart
                    : "This function can only use inputs whose meta doc part information is an instance of " + BatchMetaDocPart.class;
            BatchMetaDocPart metaDocPart = (BatchMetaDocPart) docPartData.getMetaDocPart();
            if (metaDocPart.isCreatedOnCurrentBatch()) {
                jobs.add(factory.createAddDocPartDDLJob(database, collection, metaDocPart));
                metaDocPart.streamFields()
                        .map((field) -> factory.createAddFieldDDLJob(database, collection, metaDocPart, field))
                        .forEachOrdered((job) -> jobs.add(job));
            } else {
                //it already exists, we only need to add the new fields
                for (ImmutableMetaField newField : metaDocPart.getOnBatchModifiedMetaFields()) {
                    jobs.add(factory.createAddFieldDDLJob(database, collection, metaDocPart, newField));
                }
            }

            jobs.add(factory.insert(database, collection, docPartData));
        }
        return jobs;
    }

}
