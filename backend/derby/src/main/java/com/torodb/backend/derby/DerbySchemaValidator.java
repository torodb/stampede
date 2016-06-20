package com.torodb.backend.derby;

import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Meta;
import org.jooq.Schema;
import org.jooq.Table;

import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

public class DerbySchemaValidator {
	
	private Iterable<? extends Table<?>> existingTables;
	
	public DerbySchemaValidator(Meta jooqMeta, String schemaName, String database) throws InvalidDatabaseSchemaException{
		Schema standardSchema = findSchema(jooqMeta, schemaName);
        if (standardSchema == null) {
            throw new IllegalStateException(
                    "The database "+database+" is associated with schema "
                    + schemaName+" but there is no schema with that name");
        }
        checkDatabaseSchema(standardSchema);
        this.existingTables = standardSchema.getTables();
	}
	
	private void checkDatabaseSchema(Schema schema) throws InvalidDatabaseSchemaException {
        //TODO: improve checks
    }
	
	private Schema findSchema(Meta jooqMeta, String schemaName) {
		for (Schema schema : jooqMeta.getSchemas()) {
		    if (schema.getName().equals(schemaName)) {
		        return schema;
		    }
		}
		return null;
	}

    public boolean existsTable(String tableName) {
        for (Table<?> table : existingTables) {
            if (table.getName().equals(tableName)) {
                return true;
            }
        }
        return false;
    }
    
    public boolean existsColumn(String tableName, String columnName) {
        for (Table<?> table : existingTables) {
            if (table.getName().equals(tableName)) {
                for (Field<?> field : table.fields()) {
                    if (field.getName().equals(columnName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    
    public boolean existsColumnWithType(String tableName, String columnName, DataType<?> columnType) {
        for (Table<?> table : existingTables) {
            if (table.getName().equals(tableName)) {
                for (Field<?> field : table.fields()) {
                    if (field.getName().equals(columnName)) {
                        if (field.getDataType().getSQLType() == columnType.getSQLType() &&
                            field.getDataType().getCastTypeName().equals(columnType.getCastTypeName())) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
        return false;
    }
    

    public static boolean containsField(Field<?> existingField, 
    		String collection, 
    		TableRef tableRef, 
    		Iterable<MetaFieldRecord<Object>> fields, 
    		TableRefFactory tableRefFactory) {
    	
        for (MetaFieldRecord<?> field : fields) {
            if (collection.equals(field.getCollection()) &&
                    tableRef.equals(field.getTableRefValue(tableRefFactory)) &&
                    existingField.getName().equals(field.getIdentifier())) {
                return true;
            }
        }
        return false;
    }
    
    public Iterable<? extends Table<?>> getExistingTables(){
    	return existingTables;
    }
}
