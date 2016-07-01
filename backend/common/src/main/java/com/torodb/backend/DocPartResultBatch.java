
package com.torodb.backend;

import com.torodb.core.d2r.DocPartResult;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class DocPartResultBatch implements Iterator<DocPartResult>, AutoCloseable {

    private final List<DocPartResult> docPartResults;
    private final Iterator<DocPartResult> it;

    public DocPartResultBatch(List<DocPartResult> docPartResults) {
        this.docPartResults = docPartResults;
        this.it = docPartResults.iterator();
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public DocPartResult next() {
        return it.next();
    }

    @Override
    public void close() {
        for (DocPartResult docPartResult : docPartResults) {
            docPartResult.close();
        }
    }

}
