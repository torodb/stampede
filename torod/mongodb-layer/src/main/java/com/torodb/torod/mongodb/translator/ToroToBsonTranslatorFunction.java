
package com.torodb.torod.mongodb.translator;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.google.common.base.Function;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.torod.core.subdocument.ToroDocument;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ToroToBsonTranslatorFunction implements Function<ToroDocument, BsonDocument>{

    public static final ToroToBsonTranslatorFunction INSTANCE = new ToroToBsonTranslatorFunction();

    private ToroToBsonTranslatorFunction() {
    }

    @Override
    public BsonDocument apply(ToroDocument input) {
        if (input == null) {
            return null;
        }
        return (BsonDocument) MongoWPConverter.translate(input.getRoot());
    }

}
