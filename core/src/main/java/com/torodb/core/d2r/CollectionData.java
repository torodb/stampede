package com.torodb.core.d2r;

import com.torodb.core.transaction.metainf.MetaDocPart;

public interface CollectionData<MDP extends MetaDocPart> extends Iterable<DocPartData<MDP>> {
}
