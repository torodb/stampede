package com.torodb.backend.common;

import com.torodb.backend.SqlInterface;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.*;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class AbstractMetaDataIT {

  private SqlInterface sqlInterface;

  private DatabaseTestContext dbTestContext;

  @Before
  public void setUp() throws Exception {
    dbTestContext = getDatabaseTestContext();// new DatabaseTestContextFactory().createDerbyInstance();
    sqlInterface = dbTestContext.getSqlInterface();
    dbTestContext.setupDatabase();
  }

  protected abstract DatabaseTestContext getDatabaseTestContext();

  @After
  public void tearDown() throws Exception {
    dbTestContext.tearDownDatabase();
  }



  @Test
  public void metadataDatabaseTableCanBeWritten() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      MetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database_name", "database_identifier").build();

      sqlInterface.getMetaDataWriteInterface().addMetaDatabase(dslContext, metaDatabase);

      Result<MetaDatabaseRecord> records = getDatabaseMetaTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaDatabaseRecord firstRecord = records.get(0);

      assertEquals("database_name", firstRecord.getName());
      assertEquals("database_identifier", firstRecord.getIdentifier());
    });
  }

  @Test
  public void metadataCollectionTableCanBeWritten() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      MetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database_name", "database_identifier").build();
      MetaCollection metaCollection = new ImmutableMetaCollection.Builder("collection_name", "collection_identifier").build();

      sqlInterface.getMetaDataWriteInterface().addMetaCollection(dslContext, metaDatabase, metaCollection);

      Result<MetaCollectionRecord> records = getCollectionMetaTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaCollectionRecord firstRecord = records.get(0);

      assertEquals("collection_name", firstRecord.getName());
      assertEquals("collection_identifier", firstRecord.getIdentifier());
      assertEquals("database_name", firstRecord.getDatabase());
    });
  }

  @Test
  public void metadataDocPartTableCanBeWritten() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      TableRefFactory tableRefFactory = new TableRefFactoryImpl();
      TableRef rootTableRef = tableRefFactory.createRoot();
      TableRef childTableRef = tableRefFactory.createChild(rootTableRef, "child");

      MetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database_name", "database_identifier").build();
      MetaCollection metaCollection = new ImmutableMetaCollection.Builder("collection_name", "collection_identifier").build();
      MetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(childTableRef, "docpart_identifier").build();

      sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);

      Result<MetaDocPartRecord<Object>> records = getDocPartMetaTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaDocPartRecord<Object> firstRecord = records.get(0);

      assertEquals("database_name", firstRecord.getDatabase());
      assertEquals("collection_name", firstRecord.getCollection());
      assertEquals(childTableRef, firstRecord.getTableRefValue(tableRefFactory));
      assertEquals("docpart_identifier", firstRecord.getIdentifier());
    });
  }

  private Result<MetaDatabaseRecord> getDatabaseMetaTableRecords(DSLContext dslContext) {
    MetaDatabaseTable<MetaDatabaseRecord> metaDatabaseTable = sqlInterface
        .getMetaDataReadInterface().getMetaDatabaseTable();
    return dslContext.selectFrom(metaDatabaseTable)
        .where(metaDatabaseTable.IDENTIFIER.eq("database_identifier"))
        .fetch();
  }

  private Result<MetaCollectionRecord> getCollectionMetaTableRecords(DSLContext dslContext) {
    MetaCollectionTable<MetaCollectionRecord> metaCollectionTable = sqlInterface
        .getMetaDataReadInterface().getMetaCollectionTable();
    return dslContext.selectFrom(metaCollectionTable)
        .where(metaCollectionTable.IDENTIFIER.eq("collection_identifier"))
        .fetch();
  }

  private Result<MetaDocPartRecord<Object>> getDocPartMetaTableRecords(DSLContext dslContext) {
    MetaDocPartTable<Object, MetaDocPartRecord<Object>> metaDocPartTable = sqlInterface.getMetaDataReadInterface()
        .getMetaDocPartTable();
    return dslContext.selectFrom(metaDocPartTable)
        .where(metaDocPartTable.IDENTIFIER.eq("docpart_identifier"))
        .fetch();
  }

}
