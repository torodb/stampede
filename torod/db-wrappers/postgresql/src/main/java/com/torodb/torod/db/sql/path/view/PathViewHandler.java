
package com.torodb.torod.db.sql.path.view;

import com.torodb.torod.core.exceptions.UnsupportedStructurePathViewException;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import org.jooq.*;
import org.jooq.impl.DSL;

/**
 *
 */
public abstract class PathViewHandler {

    private final TorodbMeta meta;
    private final DSLContext dsl;
    private final java.util.Comparator<Field<?>> fieldComparator;

    public PathViewHandler(TorodbMeta meta, DSLContext dsl) {
        this.meta = meta;
        this.dsl = dsl;
        this.fieldComparator = new FieldComparator();
    }

    public PathViewHandler(TorodbMeta meta, DSLContext dsl, java.util.Comparator<Field<?>> fieldComparator) {
        this.meta = meta;
        this.dsl = dsl;
        this.fieldComparator = fieldComparator;
    }

    protected abstract void createView(CreateViewFinalStep view);
    protected abstract int dropView(AttributeReference attRef, CollectionSchema colSchema);

    protected DSLContext getDsl() {
        return dsl;
    }

    public int createPathViews(String collection) throws UnsupportedStructurePathViewException {
        if (!meta.exists(collection)) {
            return 0;
        }
        CollectionSchema colSchema = meta.getCollectionSchema(collection);

        Table<AttributeReference, Integer, DocStructure> table;

        table = TableAnalyzer.analyzeCollection(colSchema);

        return createPathViews(colSchema, table);
    }

    public int dropPathViews(String collection) throws UnsupportedStructurePathViewException {
        if (!meta.exists(collection)) {
            return 0;
        }

        CollectionSchema colSchema = meta.getCollectionSchema(collection);

        Table<AttributeReference, Integer, DocStructure> table;

        table = TableAnalyzer.analyzeCollection(colSchema);

        return dropPathViews(colSchema, table);
    }

    protected String getViewName(AttributeReference attRef) {
        if (attRef.equals(AttributeReference.EMPTY_REFERENCE)) {
            return "rootView";
        }
        return attRef.toString();
    }

    private int dropPathViews(
            CollectionSchema colSchema,
            Table<AttributeReference, Integer, DocStructure> table) {
        int droppedViewCounter = 0;
        for (AttributeReference attRef : table.rowKeySet()) {
            droppedViewCounter += dropView(attRef, colSchema);
        }
        return droppedViewCounter;
    }

    protected int createPathViews(
            CollectionSchema colSchema,
            Table<AttributeReference, Integer, DocStructure> table) {

        int viewsCounter = 0;
        
        analyzeTable(table);
        
        String schemaName = colSchema.getName();
		org.jooq.Table<?> rootTable = DSL.table(DSL.name(schemaName, "root"));
		Field<Integer> rootSidField = DSL.field(DSL.name(schemaName, "root", "sid"), Integer.class);
		Field<Integer> rootDidField = DSL.field(DSL.name(schemaName, "root", "did"), Integer.class);
        
        for (AttributeReference attRef : table.rowKeySet()) {
			Select query = null;

			SortedSet<Field<?>> column = getPathFieldSet(attRef, table, colSchema);

			for (Map.Entry<Integer, DocStructure> entry : table.row(attRef).entrySet()) {
				DocStructure docStructure = entry.getValue();
				SubDocType type = docStructure.getType();
				SubDocTable subDocTable = colSchema.getSubDocTable(type);

				SortedSet<Field<?>> fields = completeFields(column, subDocTable);

				org.jooq.Table<?> joinTable = subDocTable.join(rootTable)
						.on(subDocTable.getDidColumn().eq(rootDidField));

				query = createQuery(dsl, query, fields, joinTable, docStructure, subDocTable, rootSidField, entry);
			}

			Name[] columnNames = toNameArray(column);
			
			CreateViewFinalStep view = dsl.createView(
                    DSL.name(schemaName, getViewName(attRef)),
                    columnNames
            ).as(query);
			
			createView(view);
            viewsCounter++;
        }

        return viewsCounter;
    }
    
	private SortedSet<Field<?>> completeFields(SortedSet<Field<?>> columns, SubDocTable subDocTable) {

        SortedSet<Field<?>> fields = Sets.newTreeSet(columns.comparator());

		for (Field<?> column : columns) {
			if (subDocTable.field(column.getName()) != null) {
				fields.add(subDocTable.field(column));
			} else {
				fields.add(DSL.castNull(column.getDataType()).as(column));
			}
		}

		return fields;
	}

    private SortedSet<Field<?>> getPathFieldSet(
            AttributeReference attRef,
			Table<AttributeReference, Integer, DocStructure> table,
            CollectionSchema colSchema) {

		SortedSet<Field<?>> fieldSet = Sets.newTreeSet(fieldComparator);

		for (Map.Entry<Integer, DocStructure> entry : table.row(attRef).entrySet()) {

			SubDocType type = entry.getValue().getType();
			SubDocTable subDocTable = colSchema.getSubDocTable(type);

            for (Field<?> field : subDocTable.fields()) {
                if (!field.equals(subDocTable.getIndexColumn())) {
                    fieldSet.add(field);
                }
            }
		}

		return fieldSet;
	}

    protected Name[] toNameArray(SortedSet<Field<?>> columnNamesSet) {

        Field[] asArray = new Field[columnNamesSet.size()];
        asArray = columnNamesSet.toArray(asArray);

        Name[] columnNames = new Name[asArray.length];

        for (int i = 0; i < asArray.length; i++) {
            Field field = asArray[i];
            columnNames[i] = DSL.name(field.getName());
        }

		return columnNames;
	}

    private void analyzeTable(Table<AttributeReference, Integer, DocStructure> table) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private Select createQuery(
            DSLContext dsl,
            Select query,
            Set<Field<?>> fields, org.jooq.Table<?> joinTable,
			DocStructure docStructure, 
            SubDocTable subDocTable,
            Field<Integer> rootSidField,
			Map.Entry<Integer, DocStructure> entry) {

		Condition indexCondition;

		if (docStructure.getIndex() == 0) {
			indexCondition = subDocTable.getIndexColumn().isNull();
		} else {
			indexCondition = subDocTable.getIndexColumn().eq(docStructure.getIndex());
		}

		Select subQuery = dsl.select(fields).from(joinTable).where(rootSidField.eq(DSL.val(entry.getKey()).cast(Integer.class)).and(indexCondition));

		if (query == null) {
			query = subQuery;
		} else {
			query = query.unionAll(subQuery);
		}

		return query;
	}

    private static class FieldComparator implements java.util.Comparator<Field<?>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Field<?> field1, Field<?> field2) {

            String fn1 = field1.getName();
            String fn2 = field2.getName();

            if (fn1.equals(fn2)) {
                return 0;
            }
            if (fn1.equals("did")) {
                return -1;
            } else {
                if (fn2.equals("did")) {
                    return 1;
                }
                return fn1.compareTo(fn2);
            }
        }
    }

}
