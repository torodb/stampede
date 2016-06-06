package com.torodb.core.d2r;

import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import java.util.Iterator;

public interface DocPartData extends Iterable<DocPartRow> {
    public MetaDocPart getMetaDocPart();
    public int columnCount();
    public int rowCount();
    public Iterator<MetaField> orderedMetaFieldIterator();
}
