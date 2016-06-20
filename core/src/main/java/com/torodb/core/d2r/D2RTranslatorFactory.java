
package com.torodb.core.d2r;

import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;

/**
 *
 */
@FunctionalInterface
public interface D2RTranslatorFactory {

    public D2RTranslator createTranslator(MetaDatabase database, MutableMetaCollection collection);
}
