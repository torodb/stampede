
package com.torodb.torod.db.sql.path.view;

import com.torodb.torod.core.exceptions.UnsupportedStructurePathViewException;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.AttributeReference.ObjectKey;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import java.util.Map.Entry;

/**
 *
 */
class TableAnalyzer {

    public static Table<AttributeReference, Integer, DocStructure> analyzeCollection(CollectionSchema colSchema) throws UnsupportedStructurePathViewException {
        Table<AttributeReference, Integer, DocStructure> table = HashBasedTable.create();

        for (Entry<Integer, DocStructure> entry : colSchema.getStructuresCache().getAllStructures().entrySet()) {
            analyzeStructure(
                    entry.getKey(),
                    entry.getValue(),
                    entry.getValue(),
                    table,
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
            AttributeReference path) throws UnsupportedStructurePathViewException {

        table.put(path, sid, node);

        for (Entry<String, StructureElement> entry : node.getElements().entrySet()) {
            AttributeReference newPath;
            if (entry.getValue() instanceof ArrayStructure) {
                throw new UnsupportedStructurePathViewException(root, node);
            }
            else {
                newPath = path.append(new ObjectKey(entry.getKey()));
            }
            analyzeStructure(sid, root, (DocStructure) entry.getValue(), table, newPath);
        }
    }

}
