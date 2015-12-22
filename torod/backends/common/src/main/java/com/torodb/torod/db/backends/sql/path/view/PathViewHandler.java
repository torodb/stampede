
package com.torodb.torod.db.backends.sql.path.view;

import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.torodb.torod.core.exceptions.IllegalPathViewException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.tables.SubDocTable;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 *
 */
public class PathViewHandler {

    private final TorodbMeta meta;
    private final Callback callback;
    private final java.util.Comparator<Field<?>> fieldComparator;

    public PathViewHandler(TorodbMeta meta, Callback callback) {
        this.meta = meta;
        this.callback = callback;
        this.fieldComparator = callback.getFieldComparator();
    }

    public int createPathViews(String collection) throws IllegalPathViewException {
        if (!meta.exists(collection)) {
            return 0;
        }
        IndexStorage.CollectionSchema colSchema = meta.getCollectionSchema(collection);

        Table<AttributeReference, Integer, DocStructure> table;

        table = TableAnalyzer.analyzeCollection(colSchema, false);

        return createPathViews(colSchema, table);
    }

    public void dropPathViews(String collection) throws IllegalPathViewException {
        if (!meta.exists(collection)) {
            return ;
        }

        IndexStorage.CollectionSchema colSchema = meta.getCollectionSchema(collection);

        Table<AttributeReference, Integer, DocStructure> table;

        table = TableAnalyzer.analyzeCollection(colSchema, true);

        dropPathViews(colSchema, table);
    }

    private void dropPathViews(
            IndexStorage.CollectionSchema colSchema,
            Table<AttributeReference, Integer, DocStructure> table) {
        for (AttributeReference attRef : table.rowKeySet()) {
            callback.dropView(attRef, colSchema);
        }
    }

    @SuppressWarnings("unchecked")
    protected int createPathViews(
            IndexStorage.CollectionSchema colSchema,
            Table<AttributeReference, Integer, DocStructure> table) throws IllegalPathViewException {

        int viewsCounter = 0;
        
        callback.analyzeTable(table);
        
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

				query = createQuery(
                        callback.getDsl(),
                        query,
                        fields,
                        joinTable,
                        docStructure,
                        subDocTable,
                        rootSidField,
                        entry
                );
			}

			Name[] columnNames = toNameArray(column);

            callback.dropView(attRef, colSchema);
			
            CreateViewFinalStep view = callback.getDsl().createView(
                    DSL.name(schemaName, callback.getViewName(attRef)),
                    columnNames
            ).as(query);
			
			callback.createView(view);
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
            IndexStorage.CollectionSchema colSchema) {

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

    @SuppressWarnings("unchecked")
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

        Select subQuery = dsl.select(fields).from(joinTable).where(
                rootSidField.eq(DSL.val(entry.getKey()).cast(Integer.class))
                .and(indexCondition));

        Select newQuery;
		if (query == null) {
			newQuery = subQuery;
		} else {
			newQuery = query.unionAll(subQuery);
		}

		return newQuery;
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

    public static abstract class Callback {

        abstract void createView(CreateViewFinalStep view);

        /**
         *
         * @param attRef
         * @param colSchema
         * @return the number of views that have been dropped
         */
        abstract void dropView(AttributeReference attRef, IndexStorage.CollectionSchema colSchema);

        /**
         * This method is called when view creation is requested on
         * {@link PathViewHandler} but before
         * {@link #createView(org.jooq.CreateViewFinalStep) } is called.
         * <p/>
         * It is used to stop the process if the views cannot be created.
         *
         * @param table
         * @throws IllegalPathViewException if there process must
         *                                               be stopped because
         *                                               there are paths that
         *                                               cannot be shown as
         *                                               tables
         */
        abstract void analyzeTable(Table<AttributeReference, Integer, DocStructure> table)
                throws IllegalPathViewException;

        /**
         *
         * @param path
         * @return the name of the view associated with the given path
         */
        abstract public String getViewName(AttributeReference path);

        abstract public DSLContext getDsl();

        public java.util.Comparator<Field<?>> getFieldComparator() {
            return new FieldComparator();
        }
    }

}
