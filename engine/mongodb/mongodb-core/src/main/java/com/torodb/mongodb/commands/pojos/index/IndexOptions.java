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

package com.torodb.mongodb.commands.pojos.index;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.NumberField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.torodb.mongodb.commands.pojos.index.type.AscIndexType;
import com.torodb.mongodb.commands.pojos.index.type.DescIndexType;
import com.torodb.mongodb.commands.pojos.index.type.GeoHaystackIndexType;
import com.torodb.mongodb.commands.pojos.index.type.HashedIndexType;
import com.torodb.mongodb.commands.pojos.index.type.IndexType;
import com.torodb.mongodb.commands.pojos.index.type.TextIndexType;
import com.torodb.mongodb.commands.pojos.index.type.TwoDIndexType;
import com.torodb.mongodb.commands.pojos.index.type.UnknownIndexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class IndexOptions {

  private static final String VERSION_FIELD_NAME = "v";
  private static final String NAME_FIELD_NAME = "name";
  private static final String NAMESPACE_FIELD_NAME = "ns";
  private static final String BACKGROUND_FIELD_NAME = "background";
  private static final String UNIQUE_FIELD_NAME = "unique";
  private static final String SPARSE_FIELD_NAME = "sparse";
  private static final String EXPIRE_AFTER_SECONDS_FIELD_NAME = "expireAfterSeconds";
  private static final String KEYS_FIELD_NAME = "key";
  private static final String STORAGE_ENGINE_FIELD_NAME = "storageEngine";
  private static final NumberField<?> VERSION_FIELD = new NumberField<>(VERSION_FIELD_NAME);
  private static final StringField NAME_FIELD = new StringField(NAME_FIELD_NAME);
  private static final StringField NAMESPACE_FIELD = new StringField(NAMESPACE_FIELD_NAME);
  private static final BooleanField BACKGROUND_FIELD = new BooleanField(BACKGROUND_FIELD_NAME);
  private static final BooleanField UNIQUE_FIELD = new BooleanField(UNIQUE_FIELD_NAME);
  private static final BooleanField SPARSE_FIELD = new BooleanField(SPARSE_FIELD_NAME);
  private static final IntField EXPIRE_AFTER_SECONDS_FIELD = new IntField(
      EXPIRE_AFTER_SECONDS_FIELD_NAME);
  private static final DocField KEYS_FIELD = new DocField(KEYS_FIELD_NAME);
  private static final DocField STORAGE_ENGINE_FIELD = new DocField(STORAGE_ENGINE_FIELD_NAME);
  private static final Joiner PATH_JOINER = Joiner.on('.');
  private static final Splitter PATH_SPLITER = Splitter.on('.');

  private final IndexVersion version;
  private final String name;
  @Nullable
  private final String database;
  @Nullable
  private final String collection;
  private final boolean background;
  private final boolean unique;
  private final boolean sparse;
  private final int expireAfterSeconds;
  @Nonnull
  private final BsonDocument otherProps;

  private final List<Key> keys;
  @Nonnull
  private final BsonDocument storageEngine;

  public static final Function<IndexOptions, BsonDocument> MARSHALLER_FUN = new MyMarshaller();
  public static final Function<BsonValue<?>, IndexOptions> UNMARSHALLER_FUN = new MyUnMarshaller();

  public IndexOptions(
      IndexVersion version,
      String name,
      @Nullable String database,
      @Nullable String collection,
      boolean background,
      boolean unique,
      boolean sparse,
      int expireAfterSeconds,
      @Nonnull List<Key> keys,
      @Nullable BsonDocument storageEngine,
      @Nullable BsonDocument otherProps) {
    this.version = version;
    this.name = name;
    this.database = database;
    this.collection = collection;
    this.background = background;
    this.unique = unique;
    this.sparse = sparse;
    this.expireAfterSeconds = expireAfterSeconds;
    this.keys = keys;
    this.storageEngine = storageEngine != null ? storageEngine : DefaultBsonValues.EMPTY_DOC;
    this.otherProps = otherProps != null ? otherProps : DefaultBsonValues.EMPTY_DOC;
  }

  public IndexVersion getVersion() {
    return version;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getDatabase() {
    return database;
  }

  @Nullable
  public String getCollection() {
    return collection;
  }

  /**
   * Returns a map with the indexed paths and if they are ascending or descending.
   *
   * The keys are lists of strings that represent the path of the index and values are booleans that
   * indicates if the index is ascending or descending.
   *
   * @return
   */
  public List<Key> getKeys() {
    return Collections.unmodifiableList(keys);
  }

  public boolean isBackground() {
    return background;
  }

  public boolean isUnique() {
    return unique;
  }

  public boolean isSparse() {
    return sparse;
  }

  public int getExpireAfterSeconds() {
    return expireAfterSeconds;
  }

  @Nonnull
  public BsonDocument getStorageEngine() {
    return storageEngine;
  }

  @Nonnull
  public BsonDocument getOtherProps() {
    return otherProps;
  }

  public BsonDocument marshall() {

    BsonDocumentBuilder keysDoc = new BsonDocumentBuilder();
    for (Key key : keys) {
      String path = PATH_JOINER.join(key.getKeys());
      BsonValue<?> value = key.getType().toBsonValue();
      keysDoc.appendUnsafe(path, value);
    }

    BsonDocumentBuilder builder = new BsonDocumentBuilder()
        .appendNumber(VERSION_FIELD, version.toInt())
        .append(NAME_FIELD, name)
        .append(KEYS_FIELD, keysDoc)
        .append(BACKGROUND_FIELD, background)
        .append(UNIQUE_FIELD, unique)
        .append(SPARSE_FIELD, sparse)
        .append(EXPIRE_AFTER_SECONDS_FIELD, expireAfterSeconds);
    if (!storageEngine.isEmpty()) {
      builder.append(STORAGE_ENGINE_FIELD, storageEngine);
    }
    if (database != null && collection != null) {
      builder.append(NAMESPACE_FIELD, database + '.' + collection);
    }
    for (Entry<?> otherProp : otherProps) {
      builder.appendUnsafe(otherProp.getKey(), otherProp.getValue());
    }
    return builder.build();
  }

  public static IndexOptions unmarshall(BsonDocument requestDoc)
      throws BadValueException, TypesMismatchException, NoSuchKeyException {
    IndexVersion version = IndexVersion.V1;
    String name = null;
    String namespace = null;
    boolean background = false;
    boolean unique = false;
    boolean sparse = false;
    int expireAfterSeconds = 0;
    BsonDocument keyDoc = null;
    BsonDocument storageEngine = null;
    BsonDocumentBuilder otherBuilder = new BsonDocumentBuilder();
    for (Entry<?> entry : requestDoc) {
      String key = entry.getKey();
      switch (key) {
        case VERSION_FIELD_NAME: {
          int vInt = BsonReaderTool.getNumeric(requestDoc, VERSION_FIELD).intValue();
          try {
            version = IndexVersion.fromInt(vInt);
          } catch (IndexOutOfBoundsException ex) {
            throw new BadValueException("Value " + vInt + " is not a valid version");
          }
          break;
        }
        case NAME_FIELD_NAME: {
          name = BsonReaderTool.getString(entry);
          break;
        }
        case NAMESPACE_FIELD_NAME: {
          namespace = BsonReaderTool.getString(entry);
          break;
        }
        case BACKGROUND_FIELD_NAME: {
          background = BsonReaderTool.getBooleanOrNumeric(entry, BACKGROUND_FIELD);
          break;
        }
        case UNIQUE_FIELD_NAME: {
          unique = BsonReaderTool.getBooleanOrNumeric(entry, UNIQUE_FIELD);
          break;
        }
        case SPARSE_FIELD_NAME: {
          sparse = BsonReaderTool.getBooleanOrNumeric(entry, SPARSE_FIELD);
          break;
        }
        case EXPIRE_AFTER_SECONDS_FIELD_NAME: {
          expireAfterSeconds = BsonReaderTool.getNumeric(entry, EXPIRE_AFTER_SECONDS_FIELD
              .getFieldName()).intValue();
          break;
        }
        case KEYS_FIELD_NAME: {
          keyDoc = BsonReaderTool.getDocument(entry);
          break;
        }
        case STORAGE_ENGINE_FIELD_NAME: {
          storageEngine = BsonReaderTool.getDocument(entry);
          break;
        }
        default: {
          otherBuilder.appendUnsafe(key, entry.getValue());
          break;
        }
      }
    }
    String db = null;
    String collection = null;

    if (namespace != null) {
      int dotIndex = namespace.indexOf('.');
      if (dotIndex < 1 || dotIndex > namespace.length() - 2) {
        throw new BadValueException("The not valid namespace " + namespace + " found");
      }
      db = namespace.substring(0, dotIndex);
      collection = namespace.substring(dotIndex + 1);
    }

    if (name == null) {
      throw new NoSuchKeyException(NAME_FIELD_NAME, "Indexes need names");
    }
    if (keyDoc == null) {
      throw new NoSuchKeyException(KEYS_FIELD_NAME, "Indexes need at least one key to index");
    }

    List<Key> keys = unmarshllKeys(keyDoc);

    return new IndexOptions(
        version,
        name,
        db,
        collection,
        background,
        unique,
        sparse,
        expireAfterSeconds,
        keys,
        storageEngine,
        otherBuilder.build()
    );
  }

  public static List<Key> unmarshllKeys(BsonDocument keyDoc) {
    List<Key> keys = new ArrayList<>(keyDoc.size());
    for (Entry<?> entry : keyDoc) {
      List<String> key = PATH_SPLITER.splitToList(entry.getKey());
      IndexType value = null;

      for (KnownType knownType : KnownType.values()) {
        if (knownType.getIndexType().equalsToBsonValue(entry.getValue())) {
          value = knownType.getIndexType();
          break;
        }
      }

      if (value == null) {
        value = new UnknownIndexType(entry.getValue());
      }

      keys.add(new Key(key, value));
    }
    return keys;
  }

  public static enum IndexVersion {
    V1,
    V2;

    private static IndexVersion fromInt(int i) {
      return IndexVersion.values()[i];
    }

    public int toInt() {
      return ordinal();
    }
  }

  private static class MyMarshaller implements Function<IndexOptions, BsonDocument> {

    @Override
    public BsonDocument apply(@Nonnull IndexOptions input) {
      return input.marshall();
    }

  }

  private static class MyUnMarshaller implements Function<BsonValue<?>, IndexOptions> {

    @Override
    public IndexOptions apply(@Nonnull BsonValue<?> input) {
      try {
        if (!input.isDocument()) {
          throw new IllegalArgumentException("Expected a document, "
              + "but a " + input.getType() + " was found");
        }
        return IndexOptions.unmarshall(input.asDocument());
      } catch (BadValueException ex) {
        throw new IllegalArgumentException(ex);
      } catch (TypesMismatchException ex) {
        throw new IllegalArgumentException(ex);
      } catch (NoSuchKeyException ex) {
        throw new IllegalArgumentException(ex);
      }
    }
  }

  public enum KnownType {
    asc(AscIndexType.INSTANCE),
    desc(DescIndexType.INSTANCE),
    text(TextIndexType.INSTANCE),
    twodsphere(TwoDIndexType.INSTANCE),
    geoHaystack(GeoHaystackIndexType.INSTANCE),
    twod(TwoDIndexType.INSTANCE),
    hashed(HashedIndexType.INSTANCE);

    private final IndexType indexType;

    private KnownType(IndexType indexType) {
      this.indexType = indexType;
    }

    public IndexType getIndexType() {
      return indexType;
    }

    public static boolean contains(IndexType indexType) {
      for (KnownType knownType : values()) {
        if (knownType.indexType == indexType) {
          return true;
        }
      }

      return false;
    }
  }

  public static class Key {

    private final List<String> keys;
    private final IndexType type;

    public Key(List<String> keys, IndexType type) {
      super();
      this.keys = keys;
      this.type = type;
    }

    public List<String> getKeys() {
      return keys;
    }

    public IndexType getType() {
      return type;
    }
  }

}
