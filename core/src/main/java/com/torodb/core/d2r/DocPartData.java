package com.torodb.core.d2r;

import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import java.util.Iterator;

public interface DocPartData<MDP extends MetaDocPart> extends Iterable<DocPartRow> {
    public MDP getMetaDocPart();
    public int columnCount();
    public Iterator<MetaField> orderedMetaFieldIterator();
}
