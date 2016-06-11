package com.torodb.torod.db.backends.sql.index;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.torodb.torod.core.exceptions.ExistentIndexException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.pojos.DefaultNamedToroIndex;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.IndexedAttributes.IndexType;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.pojos.UnnamedToroIndex;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.meta.CollectionSchema;
import com.torodb.torod.db.backends.meta.StructuresCache;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.tables.SubDocTable;

/**
 *
 */
public class IndexManager implements Serializable {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(IndexManager.class);
    private static final long serialVersionUID = 1L;

    private final CollectionSchema colSchema;
    private final IndexRelationManager indexRelation;
    private final String databaseName;
    private final DatabaseInterface databaseInterface;

    public IndexManager(
            CollectionSchema colSchema,
            TorodbMeta meta,
            DatabaseInterface databaseInterface) {
        this.colSchema = colSchema;
        this.databaseInterface = databaseInterface;
        this.indexRelation = new IndexRelationManager();
        this.databaseName = meta.getDatabaseName();
    }

    public void initialize(
            Set<NamedDbIndex> dbIndexes,
            Set<NamedToroIndex> toroIndexes,
            StructuresCache structures) {

        IndexRelationManager.IndexWriteTransaction indexTransaction
                = indexRelation.createWriteTransaction();
        try {
            
            for (NamedDbIndex dbIndex : dbIndexes) {
                indexTransaction.storeDbIndex(dbIndex);
            }
            for (NamedToroIndex toroIndex : toroIndexes) {
                indexTransaction.storeToroIndex(toroIndex);
            }
            
            Set<NamedDbIndex> notVisitedDbIndexes
                    = Sets.newHashSetWithExpectedSize(dbIndexes.size());
            for (NamedDbIndex dbIndex : dbIndexes) {
                notVisitedDbIndexes.add(dbIndex);
            }

            for (NamedToroIndex toroIndex : toroIndexes) {
                for (DocStructure structure : structures.getAllStructures().values()) {
                    List<UnnamedDbIndex> requiredIndexes = indexOnStructure(
                            toroIndex.asUnnamed(),
                            structure
                    );
                    if (requiredIndexes == null) {
                        continue;
                    }
                    for (UnnamedDbIndex requiredIndex : requiredIndexes) {
                        NamedDbIndex equivalentNamedDbIndex 
                                = indexTransaction.getDbIndex(requiredIndex);
                        
                        if (equivalentNamedDbIndex == null) {
                            throw new ToroRuntimeException(
                                    toroIndex + " requires a index like "
                                    + requiredIndex + " to index structure "
                                    + structure + " but that db index does not "
                                    + "exist"
                            );
                        }
                        indexTransaction.addIndexRelation(
                                toroIndex.getName(), 
                                equivalentNamedDbIndex.getName()
                        );

                        notVisitedDbIndexes.remove(equivalentNamedDbIndex);
                    }
                }
            }

            for (NamedDbIndex notVisitedDbIndex : notVisitedDbIndexes) {
                LOGGER.warn("Index {}.{} is not used.", colSchema.getName(), notVisitedDbIndex.getName());
            }
            indexTransaction.commit();
        }
        finally {
            indexTransaction.close();
        }
    }

    public Set<NamedDbIndex> getRelatedDbIndexes(String toroIndexName) {
        IndexRelationManager.IndexReadTransaction transaction
                = indexRelation.getReadTransaction();
        
        if (!transaction.existsToroIndex(toroIndexName)) {
            throw new UserToroException("There is no index named '" + toroIndexName + "'");
        }
        NamedToroIndex toroIndex = transaction.getToroIndex(toroIndexName);
        Collection<UnnamedDbIndex> unnamedDbIndexes
                = transaction.getRelatedDbIndexes(toroIndex.asUnnamed());
        Set<NamedDbIndex> result = Sets.newHashSetWithExpectedSize(unnamedDbIndexes.size());
       
        for (UnnamedDbIndex unnamedDbIndex : unnamedDbIndexes) {
            result.add(transaction.getDbIndex(unnamedDbIndex));
        }
        return result;
    }
    
    public Set<NamedToroIndex> getRelatedToroIndexes(String dbIndexName) {
        IndexRelationManager.IndexReadTransaction transaction
                = indexRelation.getReadTransaction();
        
        if (!transaction.existsDbIndex(dbIndexName)) {
            throw new ToroImplementationException(
                    "There is no db index named '" + dbIndexName + "'"
            );
        }
        NamedDbIndex dbIndex = transaction.getDbIndex(dbIndexName);
        Collection<UnnamedToroIndex> unnamedToroIndexes
                = transaction.getRelatedToroIndexes(dbIndex.asUnnamed());
        Set<NamedToroIndex> result = Sets.newHashSetWithExpectedSize(unnamedToroIndexes.size());
       
        for (UnnamedToroIndex unnamedToroIndex : unnamedToroIndexes) {
            result.add(transaction.getToroIndex(unnamedToroIndex));
        }
        return result;
    }
    
    @Nonnull
    public NamedToroIndex createIndex(
            String indexName,
            IndexedAttributes attributes,
            boolean unique,
            boolean blocking,
            DbIndexCreator dbIndexCreator,
            ToroIndexCreatedListener toroIndexListener) {

        IndexRelationManager.IndexWriteTransaction indexTransaction
                = indexRelation.createWriteTransaction();
        try {
            NamedToroIndex toroIndex = new DefaultNamedToroIndex(
                    indexName,
                    attributes,
                    databaseName,
                    colSchema.getCollection(),
                    unique
            );
            
            if (indexTransaction.existsToroIndex(indexName)) {
                NamedToroIndex oldToroIndex = indexTransaction.getToroIndex(indexName);
                if (oldToroIndex.equals(toroIndex)) {
                    throw new ExistentIndexException(indexName, colSchema.getCollection());
                }
                else {
                    throw new UserToroException(
                            "There is another index called '" + indexName + "'"
                    );
                }
            }
            
            indexTransaction.storeToroIndex(toroIndex);

            toroIndexListener.eventToroIndexCreated(toroIndex);

            Set<UnnamedDbIndex> dbUnnamedCandidates
                    = toDbIndex(toroIndex, colSchema);
            for (UnnamedDbIndex dbUnnamedCandidate : dbUnnamedCandidates) {
                NamedDbIndex dbTargetNamed
                        = indexTransaction.getDbIndex(dbUnnamedCandidate);

                if (dbTargetNamed == null) {
                    dbTargetNamed = dbIndexCreator.createIndex(
                            colSchema,
                            dbUnnamedCandidate
                    );
                    indexTransaction.storeDbIndex(dbTargetNamed);
                }
                indexTransaction.addIndexRelation(indexName, dbTargetNamed.getName());
            }
            indexTransaction.commit();
            return toroIndex;
        }
        finally {
            indexTransaction.close();
        }
    }

    private Set<UnnamedDbIndex> toDbIndex(NamedToroIndex index, CollectionSchema colSchema) {

        Set<UnnamedDbIndex> result = Sets.newHashSet();

        for (DocStructure root : colSchema.getStructuresCache().getAllStructures().values()) {
            List<UnnamedDbIndex> toDbIndexes
                    = indexOnStructure(index.asUnnamed(), root);
            if (toDbIndexes != null) {
                result.addAll(toDbIndexes);
            }
        }
        return result;
    }

    @Nullable
    private List<UnnamedDbIndex> indexOnStructure(
            UnnamedToroIndex index,
            DocStructure root) {

        IndexedAttributes indexedAtts = index.getAttributes();

        List<UnnamedDbIndex> result
                = Lists.newArrayListWithCapacity(indexedAtts.size());

        for (Map.Entry<AttributeReference, IndexType> entrySet : indexedAtts.entrySet()) {
            String tableName = getRelativeIndexedColumnInfo(
                    root,
                    entrySet.getKey()
            );
            if (tableName == null) {
                return null;
            }

            result.add(databaseInterface.getDbIndex(colSchema.getName(), tableName, entrySet));
        }
        return result;
    }

    @Nullable
    private String getRelativeIndexedColumnInfo(
            DocStructure root,
            AttributeReference attRef) {

        DocStructure lastDocStructure;
        StructureElement structureElem = root;

        final int structureLimit = attRef.getKeys().size() - 1;
        for (int i = 0; i < structureLimit; i++) {
            AttributeReference.Key key = attRef.getKeys().get(i);

            if (structureElem instanceof DocStructure) {
                if (!(key instanceof AttributeReference.ObjectKey)) {
                    return null;
                }
                AttributeReference.ObjectKey objKey
                        = (AttributeReference.ObjectKey) key;
                lastDocStructure = (DocStructure) structureElem;
                structureElem
                        = lastDocStructure.getElements().get(objKey.getKey());
                if (structureElem == null) {
                    return null;
                }
            }
            else {
                if (structureElem instanceof ArrayStructure) {
                    if (!(key instanceof AttributeReference.ArrayKey)) {
                        return null;
                    }
                    AttributeReference.ArrayKey arrKey
                            = (AttributeReference.ArrayKey) key;
                    structureElem
                            = ((ArrayStructure) structureElem).get(arrKey.getIndex());
                    if (structureElem == null) {
                        return null;
                    }
                }
                else {
                    LOGGER.warn("Unexpected structure {}", structureElem.getClass().getName());
                    return null;
                }
            }
        }
        AttributeReference.Key key = attRef.getKeys().get(structureLimit);

        if (structureElem instanceof DocStructure) {
            if (!(key instanceof AttributeReference.ObjectKey)) {
                return null;
            }
            AttributeReference.ObjectKey objKey
                    = (AttributeReference.ObjectKey) key;
            lastDocStructure = (DocStructure) structureElem;
            SubDocAttribute attribute = lastDocStructure
                    .getType()
                    .getAttribute(
                            objKey.getKey()
                    );
            if (attribute == null) {
                return null;
            }
            SubDocTable subDocTable
                    = colSchema.getSubDocTable(lastDocStructure.getType());

            return subDocTable.getName();
        }
        else {
            //TODO: keys whose last container is not an documento are not supported
            return null;
        }
    }

    public boolean dropIndex(
            String indexName,
            DbIndexDropper dbIndexDropper,
            ToroIndexDroppedListener toroIndexListener) {
        
        IndexRelationManager.IndexWriteTransaction indexTransaction
                = indexRelation.createWriteTransaction();

        try {
        
            if (!indexTransaction.existsToroIndex(indexName)) {
                return false;
            }
        
            toroIndexListener.eventToroIndexRemoved(colSchema, indexName);

            Set<NamedDbIndex> removedDbIndexes
                    = indexTransaction.removeToroIndex(indexName);

            for (NamedDbIndex dbIndex : removedDbIndexes) {
                dbIndexDropper.dropIndex(colSchema, dbIndex);
            }

            indexTransaction.commit();
            return true;
        }
        finally {
            indexTransaction.close();
        }
    }

    public void newStructureDetected(
            DocStructure newStructure,
            DbIndexCreator dbIndexCreator) {
        IndexRelationManager.IndexWriteTransaction indexTransaction
                = indexRelation.createWriteTransaction();
        try {
            for (NamedToroIndex index : indexTransaction.getToroNamedIndexes()) {
                List<UnnamedDbIndex> dbIndexes
                        = indexOnStructure(index.asUnnamed(), newStructure);
                if (dbIndexes != null) {
                    for (UnnamedDbIndex dbIndex : dbIndexes) {
                        if (!indexTransaction.exists(dbIndex)) {
                            NamedDbIndex namedDbIndex
                                    = dbIndexCreator.createIndex(colSchema, dbIndex);
                            indexTransaction.storeDbIndex(namedDbIndex);
                            indexTransaction.addIndexRelation(
                                    index.getName(), 
                                    namedDbIndex.getName()
                            );
                        }
                    }
                }
            }
            indexTransaction.commit();
        }
        finally {
            indexTransaction.close();
        }
    }

    public Collection<? extends NamedToroIndex> getIndexes() {
        return indexRelation.getReadTransaction().getToroNamedIndexes();
    }

    public static interface DbIndexCreator {

        public NamedDbIndex createIndex(
                @Nonnull CollectionSchema colSchema,
                @Nonnull UnnamedDbIndex unnamedDbIndex
        );
    }

    public static interface DbIndexDropper {

        void dropIndex(
                @Nonnull CollectionSchema colSchema,
                @Nonnull NamedDbIndex index);
    }

    public static interface ToroIndexCreatedListener {

        void eventToroIndexCreated(@Nonnull NamedToroIndex index);
    }

    public static interface ToroIndexDroppedListener {

        public void eventToroIndexRemoved(
                @Nonnull CollectionSchema colSchema,
                @Nonnull String indexName);
    }
}
