package com.torodb.d2r;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;

public class IdentifierFactoryImplTest {
	
	private IdentifierFactoryImpl identifierFactory;
	private TableRefFactory tableRefFactory= new TableRefFactoryImpl();

	@Before
	public void setUp() throws Exception {
	    this.identifierFactory = new IdentifierFactoryImpl(new MockIdentifierInterface());
	}
    
    @Test
    public void emptyDatabaseToIdentifierTest() {
        ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
        String identifier = identifierFactory.toSchemaIdentifier(metaSnapshot, "");
        Assert.assertEquals("", identifier);
    }
    
    @Test
    public void unallowedDatabaseToIdentifierTest() {
        ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
        String identifier = identifierFactory.toSchemaIdentifier(metaSnapshot, "unallowed_schema");
        Assert.assertEquals("_unallowed_schema", identifier);
    }
    
    @Test
    public void databaseToIdentifierTest() {
        ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
        String identifier = identifierFactory.toSchemaIdentifier(metaSnapshot, "database");
        Assert.assertEquals("database", identifier);
    }
    
    @Test
    public void long128DatabaseToIdentifierTest() {
        ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
        String identifier = identifierFactory.toSchemaIdentifier(metaSnapshot, 
                  "database_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
        Assert.assertEquals("database_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long", identifier);
    }
    
    @Test
    public void longForCounterDatabaseToIdentifierTest() {
        ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
        String identifier = identifierFactory.toSchemaIdentifier(metaSnapshot, 
                  "database_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
        Assert.assertEquals("database_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_1", identifier);
    }
    
    @Test
    public void longForCounterWithCollisionCharacterDatabaseToIdentifierTest() {
        ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder()
                .add(new ImmutableMetaDatabase.Builder("database_collider", 
                        "database_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_1"))
                .build();
        String identifier = identifierFactory.toSchemaIdentifier(metaSnapshot, 
                "database_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
        Assert.assertEquals("database_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_2", identifier);
    }
    
    @Test
    public void emptyCollectionDocPartRootToIdentifierTest() {
        ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
                .build();
        String identifier = identifierFactory.toTableIdentifier(metaDatabase, "", tableRefFactory.createRoot());
        Assert.assertEquals("", identifier);
    }
    
    @Test
    public void unallowedCollectionDocPartRootToIdentifierTest() {
        ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
                .build();
        String identifier = identifierFactory.toTableIdentifier(metaDatabase, "unallowed_table", tableRefFactory.createRoot());
        Assert.assertEquals("_unallowed_table", identifier);
    }
    
    @Test
    public void docPartRootToIdentifierTest() {
        ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
                .build();
        String identifier = identifierFactory.toTableIdentifier(metaDatabase, "collecti", tableRefFactory.createRoot());
        Assert.assertEquals("collecti", identifier);
    }
    
    @Test
    public void emptyDocPartToIdentifierTest() {
        ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
                .build();
        String identifier = identifierFactory.toTableIdentifier(metaDatabase, "collecti", tableRefFactory.createChild(tableRefFactory.createRoot(), ""));
        Assert.assertEquals("collecti_", identifier);
    }
    
    @Test
    public void long128DocPartToIdentifierTest() {
        ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
                .build();
        String identifier = identifierFactory.toTableIdentifier(metaDatabase, 
                "collecti", 
                tableRefFactory.createChild(
                        tableRefFactory.createRoot(), 
                            "long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long"));
        Assert.assertEquals("collecti_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long", identifier);
    }
    
    @Test
    public void longForCounterDocPartToIdentifierTest() {
        ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
                .build();
        String identifier = identifierFactory.toTableIdentifier(metaDatabase, 
                "collecti", 
                tableRefFactory.createChild(
                        tableRefFactory.createRoot(), 
                            "long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long"));
        Assert.assertEquals("collecti_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_1", identifier);
    }
    
    @Test
    public void longForCounterWithCollisionCharacterDocPartToIdentifierTest() {
        ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
                .add(new ImmutableMetaCollection.Builder("collecti", "collecti")
                        .add(new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), 
                                "collecti_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_1")
                                .build())
                        .build())
                .build();
        String identifier = identifierFactory.toTableIdentifier(metaDatabase, 
                "collecti", 
                tableRefFactory.createChild(
                        tableRefFactory.createRoot(), 
                            "long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long"));
        Assert.assertEquals("collecti_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_2", identifier);
    }
    
    @Test
    public void emptyFieldToIdentifierTest() {
        ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), 
                "docpart")
                .build();
        String identifier = identifierFactory.toFieldIdentifier(metaDocPart, FieldType.STRING, "");
        Assert.assertEquals("_s", identifier);
    }
    
    @Test
    public void unallowedFieldToIdentifierTest() {
        ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), 
                "docpart")
                .build();
        String identifier = identifierFactory.toFieldIdentifier(metaDocPart, FieldType.STRING, "unallowed_column");
        Assert.assertEquals("_unallowed_column_s", identifier);
    }
    
    @Test
    public void fieldToIdentifierTest() {
        ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), 
                "docpart")
                .build();
        String identifier = identifierFactory.toFieldIdentifier(metaDocPart, FieldType.STRING, "field");
        Assert.assertEquals("field_s", identifier);
    }
    
    @Test
    public void long128FieldToIdentifierTest() {
        ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), 
                "docpart")
                .build();
        String identifier = identifierFactory.toFieldIdentifier(metaDocPart, FieldType.STRING, 
                "field__long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
        Assert.assertEquals("field__long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_s", identifier);
    }
    
    @Test
    public void longForCounterFieldToIdentifierTest() {
        ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), 
                "docpart")
                .build();
        String identifier = identifierFactory.toFieldIdentifier(metaDocPart, FieldType.STRING, 
                "field____long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
        Assert.assertEquals("field____long_long_long_long_long_long_long_long_long_long_lonng_long_long_long_long_long_long_long_long_long_long_long_long_1_s", identifier);
    }
    
    @Test
    public void longForCounterWithCollisionCharacterFieldToIdentifierTest() {
        ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), 
                "docpart")
                .add(new ImmutableMetaField("field_collider", 
                        "field____long_long_long_long_long_long_long_long_long_long_lonng_long_long_long_long_long_long_long_long_long_long_long_long_1_s", 
                        FieldType.STRING))
                .build();
        String identifier = identifierFactory.toFieldIdentifier(metaDocPart, FieldType.STRING, 
                "field____long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
        Assert.assertEquals("field____long_long_long_long_long_long_long_long_long_long_lonng_long_long_long_long_long_long_long_long_long_long_long_long_2_s", identifier);
    }
}
