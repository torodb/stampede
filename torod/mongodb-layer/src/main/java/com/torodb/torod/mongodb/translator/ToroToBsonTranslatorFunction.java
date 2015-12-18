
package com.torodb.torod.mongodb.translator;

import com.google.common.base.Function;
import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.torod.core.subdocument.ToroDocument;
import javax.annotation.concurrent.Immutable;
import org.bson.BsonDocument;

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
        return MongoValueConverter.translateObject(input.getRoot());
    }

}
