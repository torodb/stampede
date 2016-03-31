package com.torodb.torod.mongodb.commands.impl.general.update;

import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.translator.KVToroDocument;

class MongoDocumentBuilderFactory implements DocumentBuilderFactory {

    @Override
    public ToroDocument.DocumentBuilder newDocBuilder() {
        return new KVToroDocument.Builder();
    }

}