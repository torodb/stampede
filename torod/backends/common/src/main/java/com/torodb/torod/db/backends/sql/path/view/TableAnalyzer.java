
package com.torodb.torod.db.backends.sql.path.view;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.torod.core.exceptions.IllegalPathViewException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.AttributeReference.ObjectKey;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.db.backends.meta.IndexStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;

/**
 *
 */
class TableAnalyzer {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(TableAnalyzer.class);

    private TableAnalyzer() {
    }

    public static Table<AttributeReference, Integer, DocStructure> analyzeCollection(
            IndexStorage.CollectionSchema colSchema,
            boolean ignoreArrays
    ) throws IllegalPathViewException {
        Table<AttributeReference, Integer, DocStructure> table = HashBasedTable.create();

        for (Entry<Integer, DocStructure> entry : colSchema.getStructuresCache().getAllStructures().entrySet()) {
            analyzeStructure(
                    entry.getKey(),
                    entry.getValue(),
                    entry.getValue(),
                    table,
                    ignoreArrays,
                    AttributeReference.EMPTY_REFERENCE
            );
        }

        return table;
    }

    private static void analyzeStructure(
            int sid,
            DocStructure root,
            DocStructure node,
            Table<AttributeReference, Integer, DocStructure> table,
            boolean ignoreArrays,
            AttributeReference path) throws IllegalPathViewException {

        table.put(path, sid, node);

        for (Entry<String, StructureElement> entry : node.getElements().entrySet()) {
            AttributeReference newPath;
            if (entry.getValue() instanceof ArrayStructure) {
                if (ignoreArrays) {
                    LOGGER.trace("An array structure with path {}.{}.{} because it points to an array");
                    continue;
                }
                String pathStr;
                if (path.equals(AttributeReference.EMPTY_REFERENCE)) {
                    pathStr = entry.getKey();
                }
                else {
                    pathStr = path.toString() + '.' + entry.getKey();
                }
                if (containsDocuments((ArrayStructure) entry.getValue())) {
                    throw new IllegalPathViewException("The path \"" + pathStr
                            + "\" points to an array that cointains documents "
                            + "on the structure with sid " + sid +". It is not "
                            + "possible to generate path views for that kind "
                            + "of structures");
                }
                else {
                    throw new IllegalPathViewException("The path \"" + pathStr
                            + "\" points to an array on the "
                            + "structure with sid " + sid +". It is not "
                            + "possible to generate path views for that kind "
                            + "of structures");
                }
            }
            else {
                newPath = path.append(new ObjectKey(entry.getKey()));
                analyzeStructure(sid, root, (DocStructure) entry.getValue(), table, ignoreArrays, newPath);
            }
        }
    }

    private static boolean containsDocuments(ArrayStructure arrStructure) {
        for (StructureElement value : arrStructure.getElements().values()) {
            if (value instanceof DocStructure) {
                return true;
            }
            else if (value instanceof ArrayStructure && containsDocuments((ArrayStructure) value)) {
                return true;
            }
            else {
                throw new AssertionError("Unexpected structure type");
            }
        }
        return false;
    }
}
