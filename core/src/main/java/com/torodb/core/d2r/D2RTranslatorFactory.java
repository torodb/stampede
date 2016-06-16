
package com.torodb.core.d2r;

import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.transaction.metainf.MutableMetaCollection;

/**
 *
 */
@FunctionalInterface
public interface D2RTranslatorFactory {

    public D2RTranslator createTranslator(MutableMetaCollection collection);
}
