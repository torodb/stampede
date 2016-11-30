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

package com.torodb.mongodb.commands.signatures.admin;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.FailedToParseException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.torodb.mongodb.commands.pojos.ValidationAction;
import com.torodb.mongodb.commands.pojos.ValidationLevel;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.signatures.admin.CollModCommand.CollModArgument;
import com.torodb.mongodb.commands.signatures.admin.CollModCommand.CollModResult;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 */
@Beta
public class CollModCommand extends AbstractNotAliasableCommand<CollModArgument, CollModResult> {

  public static final CollModCommand INSTANCE = new CollModCommand();

  private CollModCommand() {
    super("collMod");
  }

  @Override
  public Class<? extends CollModArgument> getArgClass() {
    return CollModArgument.class;
  }

  @Override
  public CollModArgument unmarshallArg(BsonDocument requestDoc) throws
      MongoException {
    return new CollModArgument.CollModArgumentBuilder(requestDoc)
        .build();
  }

  @Override
  protected BsonDocument marshallArg(CollModArgument request) throws
      MarshalException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Class<? extends CollModResult> getResultClass() {
    return CollModResult.class;
  }

  @Override
  public CollModResult unmarshallResult(BsonDocument resultDoc) throws
      BadValueException, TypesMismatchException, NoSuchKeyException,
      FailedToParseException, MongoException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BsonDocument marshallResult(CollModResult result) throws
      MarshalException {
    return result.marshall();
  }

  public static class CollModArgument {

    private static final DocField INDEX_FIELD = new DocField("index");
    private static final DocField VALIDATOR_FIELD = new DocField("validator");
    private static final StringField VALIDATION_LEVEL_FIELD = new StringField("validationLevel");
    private static final StringField VALIDATION_ACTION_FIELD = new StringField("validationAction");
    private static final DocField PIPELINE_FIELD = new DocField("pipeline");
    private static final StringField VIEW_ON_FIELD = new StringField("viewOn");
    private static final BooleanField USE_POWER_OF_2_SIZES_FIELD = new BooleanField(
        "usePowerOf2Sizes");
    private static final BooleanField NO_PADDING_FIELD = new BooleanField("noPadding");

    private final IndexOptions indexOptions;
    //TODO: Parse the validator
    private final BsonDocument validator;
    private final ValidationLevel validationLevel;
    private final ValidationAction validationAction;
    //TODO: Parse the pipeline
    private final BsonDocument pipeline;
    private final String viewOn;
    private final boolean usePowerOf2Sizes;
    private final boolean noPadding;

    public CollModArgument(IndexOptions indexOptions,
        BsonDocument validator,
        ValidationLevel validationLevel,
        ValidationAction validationAction,
        BsonDocument pipeline,
        String viewOn,
        boolean usePowerOf2Sizes,
        boolean noPadding) {
      this.indexOptions = indexOptions;
      this.validator = validator;
      this.validationLevel = validationLevel;
      this.validationAction = validationAction;
      this.pipeline = pipeline;
      this.viewOn = viewOn;
      this.usePowerOf2Sizes = usePowerOf2Sizes;
      this.noPadding = noPadding;
    }

    public IndexOptions getIndexOptions() {
      return indexOptions;
    }

    @Beta
    public BsonDocument getValidator() {
      return validator;
    }

    public ValidationLevel getValidationLevel() {
      return validationLevel;
    }

    public ValidationAction getValidationAction() {
      return validationAction;
    }

    @Beta
    public BsonDocument getPipeline() {
      return pipeline;
    }

    public String getViewOn() {
      return viewOn;
    }

    public boolean isUsePowerOf2Sizes() {
      return usePowerOf2Sizes;
    }

    public boolean isNoPadding() {
      return noPadding;
    }

    public static class CollModArgumentBuilder {

      private IndexOptions indexOptions;
      private BsonDocument validator;
      private ValidationLevel validationLevel;
      private ValidationAction validationAction;
      private BsonDocument pipeline;
      private String viewOn;
      private boolean usePowerOf2Sizes;
      private boolean noPadding;

      public CollModArgumentBuilder() {
      }

      public CollModArgumentBuilder(BsonDocument doc) throws TypesMismatchException,
          NoSuchKeyException, BadValueException {
        BsonDocument.Entry<?> entry;

        entry = doc.getEntry(INDEX_FIELD.getFieldName());
        if (entry != null) {
          this.indexOptions = IndexOptions.unmarshall(
              BsonReaderTool.getDocument(entry)
          );
        }

        entry = doc.getEntry(VALIDATOR_FIELD.getFieldName());
        if (entry != null) {
          this.validator = BsonReaderTool.getDocument(entry);
        }

        entry = doc.getEntry(VALIDATION_LEVEL_FIELD.getFieldName());
        if (entry != null) {
          this.validationLevel = ValidationLevel.parse(
              BsonReaderTool.getString(entry)
          );
        }

        entry = doc.getEntry(VALIDATION_ACTION_FIELD.getFieldName());
        if (entry != null) {
          this.validationAction = ValidationAction.parse(
              BsonReaderTool.getString(entry)
          );
        }

        entry = doc.getEntry(PIPELINE_FIELD.getFieldName());
        if (entry != null) {
          this.pipeline = BsonReaderTool.getDocument(entry);
        }

        entry = doc.getEntry(VIEW_ON_FIELD.getFieldName());
        if (entry != null) {
          this.viewOn = BsonReaderTool.getString(entry);
        }

        entry = doc.getEntry(USE_POWER_OF_2_SIZES_FIELD.getFieldName());
        if (entry != null) {
          this.usePowerOf2Sizes = BsonReaderTool.isPseudoTrue(entry);
        }

        entry = doc.getEntry(NO_PADDING_FIELD.getFieldName());
        if (entry != null) {
          this.noPadding = BsonReaderTool.isPseudoTrue(entry);
        }
      }

      public CollModArgumentBuilder setIndexOptions(IndexOptions indexOptions) {
        this.indexOptions = indexOptions;
        return this;
      }

      public CollModArgumentBuilder setValidator(BsonDocument validator) {
        this.validator = validator;
        return this;
      }

      public CollModArgumentBuilder setValidationLevel(ValidationLevel validationLevel) {
        this.validationLevel = validationLevel;
        return this;
      }

      public CollModArgumentBuilder setPipeline(BsonDocument pipeline) {
        this.pipeline = pipeline;
        return this;
      }

      public CollModArgumentBuilder setViewOn(String viewOn) {
        this.viewOn = viewOn;
        return this;
      }

      public CollModArgumentBuilder setUsePowerOf2Sizes(boolean usePowerOf2Sizes) {
        this.usePowerOf2Sizes = usePowerOf2Sizes;
        return this;
      }

      public CollModArgumentBuilder setNoPadding(boolean noPadding) {
        this.noPadding = noPadding;
        return this;
      }

      public CollModArgument build() {
        return new CollModArgument(indexOptions, validator,
            validationLevel, validationAction, pipeline, viewOn,
            usePowerOf2Sizes, noPadding);
      }
    }
  }

  public static class CollModResult {

    private final SortedMap<String, BsonValue<?>> oldValues;
    private final SortedMap<String, BsonValue<?>> newValues;

    private CollModResult(SortedMap<String, BsonValue<?>> oldValues,
        SortedMap<String, BsonValue<?>> newValues) {
      this.oldValues = oldValues;
      this.newValues = newValues;
    }

    public BsonValue<?> getOldValue(String property) {
      return oldValues.get(property);
    }

    public BsonValue<?> getNewValue(String property) {
      return newValues.get(property);
    }

    public BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder(oldValues.size() * 2);

      Iterator<Entry<String, BsonValue<?>>> oldEntries =
          oldValues.entrySet().iterator();
      Iterator<Entry<String, BsonValue<?>>> newEntries =
          newValues.entrySet().iterator();

      while (oldEntries.hasNext()) {
        Entry<String, BsonValue<?>> oldEntry = oldEntries.next();

        assert newEntries.hasNext() : "There are more old entries than "
            + "new ones. The expected key is " + oldEntry.getKey();

        Entry<String, BsonValue<?>> newEntry = newEntries.next();
        builder.appendUnsafe(oldEntry.getKey() + "_old", oldEntry.getValue());
        builder.appendUnsafe(newEntry.getKey() + "_new", newEntry.getValue());
      }
      assert !newEntries.hasNext() : "There are more new entries than "
          + "old ones. The expected key is " + newEntries.next().getKey();

      return builder.build();
    }

    public static class CollModResultBuilder {

      private final SortedMap<String, BsonValue<?>> oldValues = new TreeMap<>();
      private final SortedMap<String, BsonValue<?>> newValues = new TreeMap<>();
      private boolean used = false;

      public CollModResultBuilder addPropery(String propName, BsonValue<?> oldValue,
          BsonValue<?> newValue) {
        Preconditions.checkState(!used, "This builder has been used before");
        oldValues.put(propName, oldValue);
        newValues.put(propName, newValue);
        return this;
      }

      public CollModResult build() {
        this.used = true;
        return new CollModResult(oldValues, newValues);
      }
    }
  }
}
