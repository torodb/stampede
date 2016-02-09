
package com.torodb.torod.mongodb.translator;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.google.common.base.Function;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.torod.core.subdocument.ToroDocument;

/**
 *
 */
public class BsonToToroTranslatorFunction implements Function<BsonDocument, ToroDocument>{

    public static final BsonToToroTranslatorFunction INSTANCE = new BsonToToroTranslatorFunction();

    private BsonToToroTranslatorFunction() {
    }

    @Override
    public ToroDocument apply(BsonDocument input) {
        if (input == null) {
            return null;
        }
        return new BSONToroDocument((KVDocument) MongoWPConverter.translate(input));
    }

    public static class BSONToroDocument implements ToroDocument {

        private final KVDocument root;

        public BSONToroDocument(KVDocument root) {
            this.root = root;
        }

        @Override
        public KVDocument getRoot() {
            return root;
        }
    }
}
