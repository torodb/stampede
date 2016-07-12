package com.torodb.mongodb.commands.impl.general;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.google.common.base.Splitter;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVValue;

public class AttrRefHelper {

	public static KVValue<?> calculateValueAndAttRef(BsonDocument doc, AttributeReference.Builder refBuilder) throws CommandFailed {
        if (doc.size() != 1) {
            throw new CommandFailed("find", "The given query is not supported right now");
        }
        Entry<?> entry = doc.getFirstEntry();

        for (String subKey : Splitter.on('.').split(entry.getKey())) {
            refBuilder.addObjectKey(subKey);
        }

        BsonValue<?> value = entry.getValue();
		if (value.isArray()) {
            throw new CommandFailed("find", "Filters with arrays are not supported right now");
        }
        if (value.isDocument()) {
            return calculateValueAndAttRef(value.asDocument(), refBuilder);
        }
        else {
            return MongoWPConverter.translate(value);
        }
    }
}
