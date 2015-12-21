
package com.torodb.torod.db.backends.sql.path.view;

import com.google.common.collect.Table;
import com.torodb.torod.core.exceptions.IllegalPathViewException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.backends.meta.IndexStorage;
import org.jooq.CreateViewFinalStep;
import org.jooq.DSLContext;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class DefaultPathViewHandlerCallback extends PathViewHandler.Callback {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(DefaultPathViewHandlerCallback.class);
    private final DSLContext dsl;

    public DefaultPathViewHandlerCallback(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public DSLContext getDsl() {
        return dsl;
    }

    @Override
    public void analyzeTable(Table<AttributeReference, Integer, DocStructure> table)
            throws IllegalPathViewException {
        lookForIncompatibleTypes(table);
    }

    @Override
    public void createView(CreateViewFinalStep view) {
        LOGGER.info("Creating "+ view);
//        view.execute(); //See https://github.com/jOOQ/jOOQ/issues/4806
        String sql = view.getSQL(ParamType.INLINED);
        dsl.execute(sql);
    }

    @Override
    public void dropView(AttributeReference attRef, IndexStorage.CollectionSchema colSchema) {
        dsl.dropViewIfExists(
                DSL.name(
                        colSchema.getName(), getViewName(attRef)
                )
        ).execute();
    }

    @Override
    public String getViewName(AttributeReference path) {
        if (path.equals(AttributeReference.EMPTY_REFERENCE)) {
            return "vroot";
        }
        return path.toString();
    }

    private void lookForIncompatibleTypes(Table<AttributeReference, Integer, DocStructure> table)
			throws IllegalPathViewException {

		for (AttributeReference attRef : table.rowKeySet()) {

			for (Map.Entry<Integer, DocStructure> entry1 : table.row(attRef).entrySet()) {
                DocStructure structure1 = entry1.getValue();
				SubDocType type1 = structure1.getType();

				for (Map.Entry<Integer, DocStructure> entry2 : table.row(attRef).entrySet()) {
                    DocStructure structure2 = entry2.getValue();
					SubDocType type2 = structure2.getType();

					if (!type1.equals(type2)) {
						Collection<SubDocAttribute> attributes1 = type1.getAttributes();
						Collection<SubDocAttribute> attributes2 = type2.getAttributes();

						for (SubDocAttribute att1 : attributes1) {
							BasicType basicType1 = att1.getType();

							for (SubDocAttribute att2 : attributes2) {
								BasicType basicType2 = att2.getType();

								if (att1.getKey().equals(att2.getKey())) {

									if (!basicType1.equals(basicType2)
                                            && basicType1 != BasicType.NULL
                                            && basicType2 != BasicType.NULL) {
                                        String path;
                                        if (attRef.equals(AttributeReference.EMPTY_REFERENCE)) {
                                            path = att1.getKey();
                                        }
                                        else {
                                            path = attRef.toString() + '.' + att1.getKey();
                                        }
                                        throw new IllegalPathViewException(
                                                "Path \"" + path + "\" "
                                                + "points to a " + basicType1 + " "
                                                + "on structure with sid " + entry1.getKey()
                                                + " but to a " + basicType2 + " "
                                                + "on sid " + entry2.getKey()
                                        );
									}
								}
							}
						}
					}
				}
			}

		}
	}
}
