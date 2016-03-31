package com.torodb.torod.mongodb.commands.impl.general.update;

import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.connection.ToroTransaction.UpdatedToroDocument;
import com.torodb.torod.core.language.update.CompositeUpdateAction;
import com.torodb.torod.core.language.update.IncrementUpdateAction;
import com.torodb.torod.core.language.update.MoveUpdateAction;
import com.torodb.torod.core.language.update.MultiplyUpdateAction;
import com.torodb.torod.core.language.update.SetCurrentDateUpdateAction;
import com.torodb.torod.core.language.update.SetDocumentUpdateAction;
import com.torodb.torod.core.language.update.SetFieldUpdateAction;
import com.torodb.torod.core.language.update.UnsetFieldUpdateAction;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.language.update.utils.UpdateActionVisitor;
import com.torodb.torod.core.subdocument.ToroDocument;

public class UpdateActionVisitorDocumentToUpdate implements UpdateActionVisitor<UpdatedToroDocument, ToroDocument> {

    private final MongoUpdatedToroDocumentBuilder builder;
    
    public UpdateActionVisitorDocumentToUpdate(DocumentBuilderFactory documentBuilderFactory) {
        this.builder = MongoUpdatedToroDocumentBuilder.create(documentBuilderFactory);
    }
    
    @Override
    public UpdatedToroDocument visit(CompositeUpdateAction action, ToroDocument candidate) {
        builder.assign(candidate);
        
        boolean result = false;
        for (UpdateAction subAction : action.getActions().values()) {
            subAction.accept(this, candidate);
        }
        
        return builder.mergeUpdated(result).build();
    }

    @Override
    public UpdatedToroDocument visit(IncrementUpdateAction action, ToroDocument candidate) {
        builder.assign(candidate);
        
        boolean result = IncrementUpdateActionExecutor.increment(
                new ObjectBuilderCallback(builder),
                action.getModifiedField(),
                action.getDelta()
        );
        
        return builder.mergeUpdated(result).build();
    }

    @Override
    public UpdatedToroDocument visit(MultiplyUpdateAction action, ToroDocument candidate) {
        builder.assign(candidate);
        
        boolean result = MultiplyUpdateActionExecutor.multiply(
                new ObjectBuilderCallback(builder),
                action.getModifiedField(),
                action.getMultiplier()
        );
        
        return builder.mergeUpdated(result).build();
    }

    @Override
    public UpdatedToroDocument visit(
            MoveUpdateAction action,
            ToroDocument candidate
    ) {
        builder.assign(candidate);
        
        boolean result = MoveUpdateActionExecutor.move(
                new ObjectBuilderCallback(builder),
                action.getModifiedField(),
                action.getNewField()
        );
        
        return builder.mergeUpdated(result).build();
    }

    @Override
    public UpdatedToroDocument visit(SetCurrentDateUpdateAction action, ToroDocument candidate) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public UpdatedToroDocument visit(SetFieldUpdateAction action, ToroDocument candidate) {
        builder.assign(candidate);
        
        boolean result = SetFieldUpdateActionExecutor.set(
                new ObjectBuilderCallback(builder),
                action.getModifiedField(),
                action.getNewValue()
        );
        
        return builder.mergeUpdated(result).build();
    }

    @Override
    public UpdatedToroDocument visit(SetDocumentUpdateAction action, ToroDocument candidate) {
        builder.assign(candidate);
        
        boolean result = SetDocumentUpdateActionExecutor.set(
                builder,
                action.getNewValue()
        );
        
        return builder.mergeUpdated(result).build();
    }

    @Override
    public UpdatedToroDocument visit(UnsetFieldUpdateAction action, ToroDocument candidate) {
        builder.assign(candidate);
        
        boolean result = UnsetUpdateActionExecutor.unset(
                new ObjectBuilderCallback(builder),
                action.getModifiedField()
        );
        
        return builder.mergeUpdated(result).build();
    }
}
