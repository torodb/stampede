
package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.org.bson.utils.MongoBsonTranslator;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.OplogOperationParser;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.google.common.base.Charsets;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.bson.Document;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

/**
 *
 */
public class OplogOperationStreamer {

    public Stream<OplogOperation> fromFile(File f) throws IOException {
        return fromExtendedJsonStream(Files.lines(Paths.get(f.getAbsolutePath()), Charsets.UTF_8));
    }

    public Stream<OplogOperation> fromExtendedJsonStream(Stream<String> lines) {
        CodecRegistry codecRegistry = CodecRegistries.fromProviders(new DocumentCodecProvider());
        Stream<BsonDocument> docStream = lines.map(Document::parse)
                .map(doc -> doc.toBsonDocument(Document.class, codecRegistry))
                .map(MongoBsonTranslator.FROM_MONGO_FUNCTION);
        return fromBsonStream(docStream);
    }

    public Stream<OplogOperation> fromBsonStream(Stream<BsonDocument> bsonStream) {
        return bsonStream.map(OplogOperationParser.asFunction());
    }

}
