package com.torodb.core.d2r;

import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import java.util.Iterator;

public interface DocPartData extends Iterable {
    public MutableMetaDocPart getMetaDocPart();
    public int columnCount();
    public int rowCount();
    public Iterator<MetaField> orderedMetaFieldIterator();
}
