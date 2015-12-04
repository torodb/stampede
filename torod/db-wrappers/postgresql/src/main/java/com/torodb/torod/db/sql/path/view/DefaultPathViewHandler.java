
package com.torodb.torod.db.sql.path.view;

import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import org.jooq.CreateViewFinalStep;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

/**
 *
 */
public class DefaultPathViewHandler extends PathViewHandler {

    public DefaultPathViewHandler(TorodbMeta meta, DSLContext dsl) {
        super(meta, dsl);
    }

    @Override
    protected void createView(CreateViewFinalStep view) {
        view.execute();
    }

    @Override
    protected int dropView(AttributeReference attRef, CollectionSchema colSchema) {
        return getDsl().dropViewIfExists(
                DSL.name(
                        colSchema.getName(), getViewName(attRef)
                )
        ).execute();
    }

}
