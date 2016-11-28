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

package com.torodb.mongodb.commands.signatures.diagnostic;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.NumberField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand.CollStatsArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand.CollStatsReply;

import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
public class CollStatsCommand
    extends AbstractNotAliasableCommand<CollStatsArgument, CollStatsReply> {

  public static final CollStatsCommand INSTANCE = new CollStatsCommand();

  private CollStatsCommand() {
    super("collStats");
  }

  @Override
  public boolean isSlaveOk() {
    return true;
  }

  @Override
  public Class<? extends CollStatsArgument> getArgClass() {
    return CollStatsArgument.class;
  }

  @Override
  public CollStatsArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, BadValueException, NoSuchKeyException {
    return CollStatsArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(CollStatsArgument request) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @Override
  public Class<? extends CollStatsReply> getResultClass() {
    return CollStatsReply.class;
  }

  @Override
  public BsonDocument marshallResult(CollStatsReply reply) {
    return reply.marshall();
  }

  @Override
  public CollStatsReply unmarshallResult(BsonDocument resultDoc) throws
      BadValueException, TypesMismatchException, NoSuchKeyException {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @Immutable
  public static class CollStatsArgument {

    private static final StringField COLLECTION_FIELD = new StringField("collStats");
    private static final StringField LOWERCASE_COLLECTION_FIELD = new StringField(COLLECTION_FIELD
        .getFieldName().toLowerCase(Locale.ENGLISH));
    private static final NumberField SCALE_FIELD = new NumberField("scale");
    private static final BooleanField VERBOSE_FIELD = new BooleanField("verbose");
    private final String collection;
    private final int scale;
    private final boolean verbose;

    public CollStatsArgument(
        @Nonnull String collection,
        @Nonnegative int scale,
        boolean verbose) {
      this.collection = collection;
      this.scale = scale;
      this.verbose = verbose;
    }

    @Nonnegative
    public int getScale() {
      return scale;
    }

    public boolean isVerbose() {
      return verbose;
    }

    protected static CollStatsArgument unmarshall(BsonDocument doc)
        throws TypesMismatchException, BadValueException, NoSuchKeyException {
      String collection = null;
      try {
        collection = BsonReaderTool.getString(doc, COLLECTION_FIELD);
      } catch (NoSuchKeyException noSuchKeyException) {
        try {
          collection = BsonReaderTool.getString(doc, LOWERCASE_COLLECTION_FIELD);
        } catch (NoSuchKeyException noSuchLowercaseKeyException) {
          throw noSuchKeyException;
        }
      }
      int scale;
      try {
        scale = BsonReaderTool.getNumeric(doc, SCALE_FIELD, DefaultBsonValues.INT32_ONE).intValue();
      } catch (TypesMismatchException typesMismatchException) {
        scale = 1;
      }
      if (scale <= 0) {
        throw new BadValueException("Scale must be a value >= 1");
      }
      boolean verbose = BsonReaderTool.getBooleanOrNumeric(doc, VERBOSE_FIELD, false);

      return new CollStatsArgument(collection, scale, verbose);
    }

    public String getCollection() {
      return collection;
    }

  }

  //TODO(gortiz): This reply is not prepared to respond on error cases!
  public static class CollStatsReply {

    private static final StringField NS_FIELD = new StringField("ns");
    private static final NumberField<?> COUNT_FIELD = new NumberField<>("count");
    private static final NumberField<?> SIZE_FIELD = new NumberField<>("size");
    private static final NumberField<?> AVG_OBJ_SIZE_FIELD = new NumberField<>("avgObjSize");
    private static final NumberField<?> STORAGE_SIZE_FIELD = new NumberField<>("storageSize");
    private static final IntField N_INDEXES_FIELD = new IntField("nindexes");
    private static final DocField INDEX_DETAILS_FIELD = new DocField("indexDetails");
    @SuppressWarnings("checkstyle:LineLength")
    private static final NumberField<?> TOTAL_INDEX_SIZE_FIELD = new NumberField<>("totalIndexSize");
    private static final DocField INDEX_SIZES_FIELD = new DocField("indexSizes");
    private static final BooleanField CAPPED_FIELD = new BooleanField("capped");
    private static final NumberField<?> MAX_FIELD = new NumberField<>("max");

    private final int scale;
    @Nonnull
    private final String database;
    @Nonnull
    private final String collection;
    @Nonnull
    private final Number count;
    @Nonnull
    private final Number size;
    @Nonnull
    private final Number storageSize;
    @Nullable
    private final BsonDocument customStorageStats;
    private final boolean capped;
    @Nullable
    private final Number maxIfCapped;
    @Nonnull
    private final BsonDocument indexDetails;
    @Nonnull
    private final ImmutableMap<String, ? extends Number> sizeByIndex;

    public CollStatsReply(
        @Nonnegative int scale,
        String database,
        String collection,
        Number count,
        Number size,
        Number storageSize,
        @Nullable BsonDocument customStorageStats,
        boolean capped,
        Number maxIfCapped,
        BsonDocument indexDetails,
        ImmutableMap<String, ? extends Number> sizeByIndex) {
      this.scale = scale;
      this.database = database;
      this.collection = collection;
      this.count = count;
      this.size = size;
      this.storageSize = storageSize;
      this.customStorageStats = customStorageStats;
      this.capped = capped;
      this.maxIfCapped = maxIfCapped;
      this.indexDetails = indexDetails;
      this.sizeByIndex = sizeByIndex;
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();
      builder.append(NS_FIELD, database + '.' + collection);
      builder.appendNumber(COUNT_FIELD, count);
      builder.appendNumber(SIZE_FIELD, size);
      if (count.longValue() != 0) {
        Number avgObjSize = scale * size.longValue() / count.longValue();
        builder.appendNumber(AVG_OBJ_SIZE_FIELD, avgObjSize);
      }
      builder.appendNumber(STORAGE_SIZE_FIELD, storageSize);
      builder.append(N_INDEXES_FIELD, sizeByIndex.size());
      builder.append(INDEX_DETAILS_FIELD, indexDetails);
      builder.appendNumber(TOTAL_INDEX_SIZE_FIELD, getTotalIndexSize());
      builder.append(INDEX_SIZES_FIELD, marshallSizeByIndex(sizeByIndex));
      builder.append(CAPPED_FIELD, capped);
      if (maxIfCapped != null) {
        builder.appendNumber(MAX_FIELD, maxIfCapped);
      }

      return builder.build();
    }

    private Number getTotalIndexSize() {
      long totalSize = 0;
      for (Number indexSize : sizeByIndex.values()) {
        totalSize += indexSize.longValue();
      }
      return totalSize;
    }

    private BsonDocument marshallSizeByIndex(ImmutableMap<String, ? extends Number> sizeByIndex) {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();
      for (Entry<String, ? extends Number> entry : sizeByIndex.entrySet()) {
        builder.appendNumber(new NumberField(entry.getKey()), entry.getValue());
      }
      return builder.build();
    }

    public static class Builder {

      private int scale;
      @Nonnull
      private final String database;
      @Nonnull
      private final String collection;
      private Number count;
      private Number size;
      private Number storageSize;
      @Nullable
      private BsonDocument customStorageStats;
      private boolean capped;
      @Nullable
      private Number maxIfCapped;
      private BsonDocument indexDetails;
      private Map<String, ? extends Number> sizeByIndex;

      public Builder(@Nonnull String database, @Nonnull String collection) {
        this.database = database;
        this.collection = collection;
      }

      public int getScale() {
        return scale;
      }

      public Builder setScale(int scale) {
        Preconditions.checkArgument(scale > 0, "Scale must be a positive number");
        this.scale = scale;
        return this;
      }

      public Number getCount() {
        return count;
      }

      /**
       *
       * @param count The number of objects or documents in this collection.
       * @return
       */
      public Builder setCount(@Nonnull @Nonnegative Number count) {
        this.count = count;
        return this;
      }

      public Number getSize() {
        return size;
      }

      /**
       * The total size of all records in a collection. This value does not include the record
       * header, which is 16 bytes per record, but does include the record’s padding. Additionally
       * size does not include the size of any indexes associated with the collection.
       * <p>
       * The scale argument affects this value.
       * <p>
       * @param size
       * @return
       */
      public Builder setSize(@Nonnull @Nonnegative Number size) {
        this.size = size;
        return this;
      }

      public Number getStorageSize() {
        return storageSize;
      }

      /**
       * The total amount of storage allocated to this collection for document storage. The scale
       * argument affects this value. The storageSize does not decrease as you remove or shrink
       * documents.
       * <p>
       * @param storageSize
       * @return
       */
      public Builder setStorageSize(@Nonnull @Nonnegative Number storageSize) {
        this.storageSize = storageSize;
        return this;
      }

      public boolean isCapped() {
        return capped;
      }

      /**
       * This field will be “true” if the collection is capped.
       * <p>
       * @param capped
       * @return
       */
      public Builder setCapped(boolean capped) {
        this.capped = capped;
        return this;
      }

      public Number getMaxIfCapped() {
        return maxIfCapped;
      }

      /**
       * Shows the maximum number of documents that may be present in a capped collection.
       * <p>
       * @param maxIfCapped
       * @return
       */
      public Builder setMaxIfCapped(@Nullable @Nonnegative Number maxIfCapped) {
        this.maxIfCapped = maxIfCapped;
        return this;
      }

      public Map<String, ? extends Number> getSizeByIndex() {
        return sizeByIndex;
      }

      /**
       * This field specifies the key and size of every existing index on the collection. The scale
       * argument affects this value.
       * <p>
       * @param sizeByIndex
       * @return
       */
      public Builder setSizeByIndex(@Nonnull Map<String, ? extends Number> sizeByIndex) {
        this.sizeByIndex = sizeByIndex;
        return this;
      }

      public BsonDocument getCustomStorageStats() {
        return customStorageStats;
      }

      public Builder setCustomStorageStats(@Nullable BsonDocument customStorageStats) {
        this.customStorageStats = customStorageStats;
        return this;
      }

      public BsonDocument getIndexDetails() {
        return indexDetails;
      }

      public Builder setIndexDetails(@Nonnull BsonDocument indexDetails) {
        this.indexDetails = indexDetails;
        return this;
      }

      public CollStatsReply build() {
        assert scale > 0;
        assert database != null;
        assert collection != null;
        assert count != null;
        assert size != null;
        assert storageSize != null;
        assert indexDetails != null;
        assert sizeByIndex != null;

        return new CollStatsReply(
            scale,
            database,
            collection,
            count,
            size,
            storageSize,
            customStorageStats,
            capped,
            maxIfCapped,
            indexDetails,
            ImmutableMap.copyOf(sizeByIndex)
        );
      }
    }

  }

}
