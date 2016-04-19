package com.torodb.torod.mongodb.commands.impl.general.update;

import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.heap.ByteArrayKVMongoObjectId;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.language.update.SetDocumentUpdateAction;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.language.update.utils.UpdateActionVisitorAdaptor;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.repl.ObjectIdFactory;

public class UpdateActionVisitorDocumentToInsert extends UpdateActionVisitorAdaptor<ToroDocument, Void> {
    private final DocumentBuilderFactory documentBuilderFactory;
    private final ObjectIdFactory objectIdFactory;
    
    public UpdateActionVisitorDocumentToInsert(DocumentBuilderFactory documentBuilderFactory,
            ObjectIdFactory objectIdFactory) {
        super();
        this.documentBuilderFactory = documentBuilderFactory;
        this.objectIdFactory = objectIdFactory;
    }
    
    @Override
    public ToroDocument defaultCase(UpdateAction action, Void arg) {
        throw new ToroRuntimeException();
    }
    @Override
    public ToroDocument visit(SetDocumentUpdateAction action, Void arg) {
        KVDocument.Builder kvDocumentBuilder = new KVDocument.Builder();
        for (DocEntry<?> docEntry : action.getNewValue()) {
            kvDocumentBuilder.putValue(docEntry.getKey(), docEntry.getValue());
        }
        if (!action.getNewValue().containsKey("_id")) {
            kvDocumentBuilder.putValue("_id", 
                    new ByteArrayKVMongoObjectId(objectIdFactory.consumeObjectId().toByteArray()));
        }
        return documentBuilderFactory.newDocBuilder()
                .setRoot(kvDocumentBuilder.build())
                .build();
    }
}
