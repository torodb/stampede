
package com.torodb.torod.db.sql.delegator;

import com.google.common.collect.Sets;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.pojos.DefaultNamedToroIndex;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;
import com.torodb.torod.db.sql.index.IndexManager;
import com.torodb.torod.db.sql.index.IndexedColumnInfo;
import com.torodb.torod.db.sql.index.NamedDbIndex;
import com.torodb.torod.db.sql.index.UnnamedDbIndex;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class CreateIndexDelegator {

    private final IndexManager indexManager;
    private final TorodbMeta meta;
    private final NamedDbIndexCreator namedDbIndexCreator;

    @Inject
    public CreateIndexDelegator(
            IndexManager indexManager, 
            TorodbMeta meta, 
            NamedDbIndexCreator namedDbIndexCreator) {
        this.indexManager = indexManager;
        this.meta = meta;
        this.namedDbIndexCreator = namedDbIndexCreator;
    }

    public NamedToroIndex createIndex(
            String collection,
            String indexName, 
            IndexedAttributes attributes, 
            boolean unique, 
            boolean blocking) {
        CollectionSchema colSchema = meta.getCollectionSchema(collection);
        
        indexManager.getWriteLock().lock();
        try {
            if (indexManager.existsToroIndex(indexName)) {
                throw new IllegalArgumentException(
                        "There is another index called '"+indexName+"'"
                );
            }

            DefaultNamedToroIndex toroIndex = new DefaultNamedToroIndex(
                    indexName, 
                    attributes, 
                    meta.getDatabaseName(), 
                    colSchema.getCollection(), 
                    unique
            );

            if (indexManager.existsToroIndex(toroIndex.asUnnamed())) {
                throw new IllegalArgumentException("There is another index "
                        + "simmilar to '"+toroIndex.asUnnamed()+"'"
                );
            }

            Set<UnnamedDbIndex> dbUnnamedCandidates = toDbIndex(toroIndex, colSchema);
            for (UnnamedDbIndex dbUnnamedCandidate : dbUnnamedCandidates) {
                NamedDbIndex dbTargetNamed = indexManager.getDbIndex(dbUnnamedCandidate);

                if (dbTargetNamed == null) {
                    dbTargetNamed = namedDbIndexCreator.createDbIndex(
                            toroIndex, 
                            dbUnnamedCandidate
                    );
                }
                indexManager.addIndexRelation(toroIndex, dbTargetNamed);
            }
            return toroIndex;
        } finally {
            indexManager.getWriteLock().unlock();
        }
    }
    
    private Set<UnnamedDbIndex> toDbIndex(NamedToroIndex index, CollectionSchema colSchema) {
        
        Set<UnnamedDbIndex> result = Sets.newHashSet();
        
        UnnamedDbIndex.Builder builder = new UnnamedDbIndex.Builder();
        for (DocStructure root : colSchema.getStructuresCache().getAllStructures().values()) {
            builder.clear();
            
            builder.setSchema(colSchema.getName());
            builder.setUnique(index.isUnique());
            
            if (toDbIndex(index.getAttributes(), colSchema, builder, root)) {
                UnnamedDbIndex newIndex = builder.build();
                result.add(newIndex);
            }
        }
        return result;
    }
    
    private boolean toDbIndex(
            IndexedAttributes indexedAtts, 
            CollectionSchema colSchema,
            UnnamedDbIndex.Builder builder, 
            DocStructure root) {
        
        for (Map.Entry<AttributeReference, Boolean> entrySet : indexedAtts.entrySet()) {
            IndexedColumnInfo column = getRelativeIndexedColumnInfo(
                    root, 
                    colSchema,
                    entrySet.getKey(), 
                    entrySet.getValue()
            );
            if (column == null) {
                return false;
            }
            builder.addColumn(column);
        }
        return true;
    }
    
    @Nullable
    private IndexedColumnInfo getRelativeIndexedColumnInfo(
            DocStructure root, 
            CollectionSchema colSchema,
            AttributeReference attRef, 
            boolean ascending) {
        
        DocStructure lastDocStructure;
        StructureElement structureElem = root;
        
        final int structureLimit = attRef.getKeys().size() - 1;
        for (int i = 0; i < structureLimit; i++) {
            AttributeReference.Key key = attRef.getKeys().get(i);
            
            if (structureElem instanceof DocStructure) {
                if (!(key instanceof AttributeReference.ObjectKey)) {
                    return null;
                }
                AttributeReference.ObjectKey objKey = (AttributeReference.ObjectKey) key;
                lastDocStructure = (DocStructure) structureElem;
                structureElem = lastDocStructure.getElements().get(objKey.getKey());
                if (structureElem == null) {
                    return null;
                }
            }
            else {
                if (structureElem instanceof ArrayStructure) {
                    if (!(key instanceof AttributeReference.ArrayKey)) {
                        return null;
                    }
                    AttributeReference.ArrayKey arrKey = (AttributeReference.ArrayKey) key;
                    structureElem = ((ArrayStructure) structureElem).get(arrKey.getIndex());
                    if (structureElem == null) {
                        return null;
                    }
                }
                else {
                    return null;
                }
            }
        }
        AttributeReference.Key key = attRef.getKeys().get(structureLimit);
        
        if (structureElem instanceof DocStructure) {
            if (!(key instanceof AttributeReference.ObjectKey)) {
                return null;
            }
            AttributeReference.ObjectKey objKey = (AttributeReference.ObjectKey) key;
            lastDocStructure = (DocStructure) structureElem;
            SubDocAttribute attribute = lastDocStructure
                    .getType()
                    .getAttribute(
                            objKey.getKey()
                    );
            if (attribute == null) {
                return null;
            }
            SubDocTable subDocTable = colSchema.getSubDocTable(lastDocStructure.getType());
            String columnName = SubDocTable.toColumnName(attribute.getKey());
            
            return new IndexedColumnInfo(subDocTable.getName(), columnName, ascending);
        }
        else {
            //TODO: keys whose last container is not an documento are not supported
            return null;
        }
    }
    
    
    public static interface NamedDbIndexCreator {
        public NamedDbIndex createDbIndex(NamedToroIndex toroIndex, UnnamedDbIndex unnamedDbIndex);
    }
}
