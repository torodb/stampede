
package com.torodb.torod.mongodb.translator;

import com.google.common.base.Function;
import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.torod.core.subdocument.ToroDocument;
import org.bson.BsonDocument;

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
        return new BSONToroDocument(MongoValueConverter.translateObject(input));
    }

    public static class BSONToroDocument implements ToroDocument {

        private final ObjectValue root;

        public BSONToroDocument(ObjectValue root) {
            this.root = root;
        }

        @Override
        public ObjectValue getRoot() {
            return root;
        }
    }
}
