package com.torodb.core.d2r;

import java.util.Iterator;

import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;

public interface DocPartData extends Iterable<DocPartRow> {
    MetaDocPart getMetaDocPart();
    int fieldColumnsCount();
    int scalarColumnsCount();
    int rowCount();
    Iterator<MetaField> orderedMetaFieldIterator();
    Iterator<MetaScalar> orderedMetaScalarIterator();
}
