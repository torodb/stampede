/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.repl.oplogreplier.analyzed;

import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.exceptions.user.UpdateException;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.MapKvDocument;
import com.torodb.mongodb.language.Constants;
import com.torodb.mongodb.language.UpdateActionTranslator;
import com.torodb.mongodb.language.update.CompositeUpdateAction;
import com.torodb.mongodb.language.update.IncrementUpdateAction;
import com.torodb.mongodb.language.update.MoveUpdateAction;
import com.torodb.mongodb.language.update.MultiplyUpdateAction;
import com.torodb.mongodb.language.update.SetCurrentDateUpdateAction;
import com.torodb.mongodb.language.update.SetDocumentUpdateAction;
import com.torodb.mongodb.language.update.SetFieldUpdateAction;
import com.torodb.mongodb.language.update.SingleFieldUpdateAction;
import com.torodb.mongodb.language.update.UnsetFieldUpdateAction;
import com.torodb.mongodb.language.update.UpdateAction;
import com.torodb.mongodb.language.update.UpdateActionVisitor;
import com.torodb.mongodb.language.update.UpdatedToroDocumentBuilder;

import javax.inject.Singleton;

@Singleton
public class UpdateActionsTool {

  private static final MergerProvider MERGER_PROVIDER = new MergerProvider();
  private static final SingleFieldMerger SINGLE_FIELD_MERGER = new SingleFieldMerger();
  private static final CompositeMerger COMPOSITE_MERGER = new CompositeMerger();
  private static final SetDocMerger SET_DOC_MERGER = new SetDocMerger();

  private UpdateActionsTool() {
  }

  public static KvDocument applyAsUpsert(UpdateOplogOperation updateAction) {
    @SuppressWarnings("checkstyle:LocalVariableName")
    KvValue<?> _id = MongoWpConverter.translate(updateAction.getDocId());
    KvDocument initialDoc = new MapKvDocument.Builder()
        .putValue(Constants.ID, _id)
        .build();
    return applyModification(initialDoc, parseUpdateAction(updateAction));
  }

  public static UpdateAction parseUpdateAction(UpdateOplogOperation updateAction) {
    try {
      return UpdateActionTranslator.translate(updateAction.getModification());
    } catch (UpdateException ex) {
      throw new ToroRuntimeException("Unexpected error while analyzing a update oplog operation",
          ex);
    }
  }

  public static KvDocument applyModification(KvDocument doc, UpdateAction modification) {
    UpdatedToroDocumentBuilder docBuilder = UpdatedToroDocumentBuilder.from(doc);
    try {
      modification.apply(docBuilder);
    } catch (UpdateException ex) {
      throw new ToroRuntimeException("Unexpected error while analyzing a update oplog operation",
          ex);
    }
    return docBuilder.build();
  }

  @SuppressWarnings("unchecked")
  public static UpdateAction mergeModifications(UpdateAction mod1, UpdateAction mod2) {
    UpdateActionVisitor merger = mod1.accept(MERGER_PROVIDER, null);
    return (UpdateAction) mod2.accept(merger, mod1);
  }

  static boolean isSetModification(UpdateOplogOperation op) {
    //TODO: This method could be implemented more efficiently
    UpdateAction updateAction = parseUpdateAction(op);
    return updateAction.isSetModification();
  }

  private static class MergerProvider implements
      UpdateActionVisitor<UpdateActionVisitor<UpdateAction, ? extends UpdateAction>, Void> {

    @Override
    public UpdateActionVisitor<UpdateAction, ? extends UpdateAction> visit(
        CompositeUpdateAction newAction, Void arg) {
      return COMPOSITE_MERGER;
    }

    @Override
    public UpdateActionVisitor<UpdateAction, ? extends UpdateAction> visit(
        IncrementUpdateAction newAction, Void arg) {
      return SINGLE_FIELD_MERGER;
    }

    @Override
    public UpdateActionVisitor<UpdateAction, ? extends UpdateAction> visit(
        MoveUpdateAction newAction, Void arg) {
      return SINGLE_FIELD_MERGER;
    }

    @Override
    public UpdateActionVisitor<UpdateAction, ? extends UpdateAction> visit(
        MultiplyUpdateAction newAction, Void arg) {
      return SINGLE_FIELD_MERGER;
    }

    @Override
    public UpdateActionVisitor<UpdateAction, ? extends UpdateAction> visit(
        SetCurrentDateUpdateAction newAction, Void arg) {
      return SINGLE_FIELD_MERGER;
    }

    @Override
    public UpdateActionVisitor<UpdateAction, ? extends UpdateAction> visit(
        SetDocumentUpdateAction newAction, Void arg) {
      return SET_DOC_MERGER;
    }

    @Override
    public UpdateActionVisitor<UpdateAction, ? extends UpdateAction> visit(
        SetFieldUpdateAction newAction, Void arg) {
      return SINGLE_FIELD_MERGER;
    }

    @Override
    public UpdateActionVisitor<UpdateAction, ? extends UpdateAction> visit(
        UnsetFieldUpdateAction newAction, Void arg) {
      return SINGLE_FIELD_MERGER;
    }

  }

  private static class SingleFieldMerger implements
      UpdateActionVisitor<UpdateAction, SingleFieldUpdateAction> {

    @Override
    public UpdateAction visit(CompositeUpdateAction newAction, SingleFieldUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      builder.add(oldAction, true);
      newAction.getActions().values().forEach((subAction) -> builder.add(subAction, true));
      return builder.build();
    }

    @Override
    public UpdateAction visit(IncrementUpdateAction newAction, SingleFieldUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      builder.add(oldAction, true);
      builder.add(newAction, true);
      return builder.build();
    }

    @Override
    public UpdateAction visit(MoveUpdateAction newAction, SingleFieldUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      builder.add(oldAction, true);
      builder.add(newAction, true);
      return builder.build();
    }

    @Override
    public UpdateAction visit(MultiplyUpdateAction newAction, SingleFieldUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      builder.add(oldAction, true);
      builder.add(newAction, true);
      return builder.build();
    }

    @Override
    public UpdateAction visit(SetCurrentDateUpdateAction newAction,
        SingleFieldUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      builder.add(oldAction, true);
      builder.add(newAction, true);
      return builder.build();
    }

    @Override
    public UpdateAction visit(SetDocumentUpdateAction newAction,
        SingleFieldUpdateAction oldAction) {
      return newAction;
    }

    @Override
    public UpdateAction visit(SetFieldUpdateAction newAction, SingleFieldUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      builder.add(oldAction, true);
      builder.add(newAction, true);
      return builder.build();
    }

    @Override
    public UpdateAction visit(UnsetFieldUpdateAction newAction, SingleFieldUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      builder.add(oldAction, true);
      builder.add(newAction, true);
      return builder.build();
    }

  }

  private static class CompositeMerger implements
      UpdateActionVisitor<UpdateAction, CompositeUpdateAction> {

    @Override
    public UpdateAction visit(CompositeUpdateAction newAction, CompositeUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      oldAction.getActions().values().forEach(subAction -> builder.add(subAction, true));
      newAction.getActions().values().forEach(subAction -> builder.add(subAction, true));

      return builder.build();
    }

    @Override
    public UpdateAction visit(IncrementUpdateAction newAction, CompositeUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      oldAction.getActions().values().forEach(subAction -> builder.add(subAction, true));
      builder.add(newAction, true);

      return builder.build();
    }

    @Override
    public UpdateAction visit(MoveUpdateAction newAction, CompositeUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      oldAction.getActions().values().forEach(subAction -> builder.add(subAction, true));
      builder.add(newAction, true);

      return builder.build();
    }

    @Override
    public UpdateAction visit(MultiplyUpdateAction newAction, CompositeUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      oldAction.getActions().values().forEach(subAction -> builder.add(subAction, true));
      builder.add(newAction, true);

      return builder.build();
    }

    @Override
    public UpdateAction visit(SetCurrentDateUpdateAction newAction,
        CompositeUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      oldAction.getActions().values().forEach(subAction -> builder.add(subAction, true));
      builder.add(newAction, true);

      return builder.build();
    }

    @Override
    public UpdateAction visit(SetDocumentUpdateAction newAction, CompositeUpdateAction oldAction) {
      return newAction;
    }

    @Override
    public UpdateAction visit(SetFieldUpdateAction newAction, CompositeUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      oldAction.getActions().values().forEach(subAction -> builder.add(subAction, true));
      builder.add(newAction, true);

      return builder.build();
    }

    @Override
    public UpdateAction visit(UnsetFieldUpdateAction newAction, CompositeUpdateAction oldAction) {
      CompositeUpdateAction.Builder builder = new CompositeUpdateAction.Builder();

      oldAction.getActions().values().forEach(subAction -> builder.add(subAction, true));
      builder.add(newAction, true);

      return builder.build();
    }

  }

  private static class SetDocMerger implements
      UpdateActionVisitor<UpdateAction, SetDocumentUpdateAction> {

    @Override
    public UpdateAction visit(CompositeUpdateAction newAction, SetDocumentUpdateAction oldAction) {
      KvDocument newDoc = applyModification(oldAction.getNewValue(), newAction);
      return new SetDocumentUpdateAction(newDoc);
    }

    @Override
    public UpdateAction visit(IncrementUpdateAction newAction, SetDocumentUpdateAction oldAction) {
      KvDocument newDoc = applyModification(oldAction.getNewValue(), newAction);
      return new SetDocumentUpdateAction(newDoc);
    }

    @Override
    public UpdateAction visit(MoveUpdateAction newAction, SetDocumentUpdateAction oldAction) {
      KvDocument newDoc = applyModification(oldAction.getNewValue(), newAction);
      return new SetDocumentUpdateAction(newDoc);
    }

    @Override
    public UpdateAction visit(MultiplyUpdateAction newAction, SetDocumentUpdateAction oldAction) {
      KvDocument newDoc = applyModification(oldAction.getNewValue(), newAction);
      return new SetDocumentUpdateAction(newDoc);
    }

    @Override
    public UpdateAction visit(SetCurrentDateUpdateAction newAction,
        SetDocumentUpdateAction oldAction) {
      KvDocument newDoc = applyModification(oldAction.getNewValue(), newAction);
      return new SetDocumentUpdateAction(newDoc);
    }

    @Override
    public UpdateAction visit(SetDocumentUpdateAction newAction,
        SetDocumentUpdateAction oldAction) {
      return newAction;
    }

    @Override
    public UpdateAction visit(SetFieldUpdateAction newAction, SetDocumentUpdateAction oldAction) {
      KvDocument newDoc = applyModification(oldAction.getNewValue(), newAction);
      return new SetDocumentUpdateAction(newDoc);
    }

    @Override
    public UpdateAction visit(UnsetFieldUpdateAction newAction, SetDocumentUpdateAction oldAction) {
      KvDocument newDoc = applyModification(oldAction.getNewValue(), newAction);
      return new SetDocumentUpdateAction(newDoc);
    }

  }
}
