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

package com.torodb.backend.meta;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.meta.SchemaValidator.Table.ResultSetIterator;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.backend.tables.records.MetaScalarRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.exceptions.SystemException;
import org.jooq.DSLContext;
import org.jooq.DataType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SchemaValidator {

  private static final Splitter COLUMN_TYPE_NAME_SPLITTER = Splitter.on('.');

  private final String database;
  private final String schemaName;
  private final Iterable<? extends Table> existingTables;
  private final Iterable<? extends Index> existingIndexes;

  public SchemaValidator(DSLContext dsl, String schemaName, String database) throws
      InvalidDatabaseSchemaException {
    this.database = database;
    this.schemaName = schemaName;
    Connection connection = dsl.configuration().connectionProvider().acquire();
    try {
      checkDatabaseSchema(connection);
      existingTables = getTables(schemaName, connection);
      existingIndexes = getIndexes(schemaName, existingTables, connection);
    } finally {
      dsl.configuration().connectionProvider().release(connection);
    }
  }

  private List<Table> getTables(String schemaName, Connection connection) {
    List<Table> tables = new ArrayList<>();
    Table.ResultSetIterator tableIterator =
        getTableIterator(schemaName, connection);
    while (tableIterator.hasNext()) {
      Table table = tableIterator.next();
      tables.add(table);
    }
    return tables;
  }

  protected ResultSetIterator getTableIterator(String schemaName, Connection connection) {
    return new Table.ResultSetIterator(schemaName, connection);
  }

  private List<Index> getIndexes(String schemaName, Iterable<? extends Table> existingTables,
      Connection connection) {
    List<Index> indexes = new ArrayList<>();
    for (Table table : existingTables) {
      Index.ResultSetIterator indexIterator =
          getIndexIterator(schemaName, connection, table);
      while (indexIterator.hasNext()) {
        Index index = indexIterator.next();
        indexes.add(index);
      }
    }
    return indexes;
  }

  protected com.torodb.backend.meta.SchemaValidator.Index.ResultSetIterator getIndexIterator(
      String schemaName,
      Connection connection, Table table) {
    return new Index.ResultSetIterator(schemaName, table.getName(), connection);
  }

  private void checkDatabaseSchema(Connection connection) throws InvalidDatabaseSchemaException {
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getSchemas();
      while (resultSet.next()) {
        if (resultSet.getString("TABLE_SCHEM").equals(schemaName)) {
          return;
        }
      }

      throw new IllegalStateException(
          "The database " + database + " is associated with schema "
          + schemaName + " but there is no schema with that name");
    } catch (SQLException sqlException) {
      throw new SystemException(sqlException);
    }
  }

  public boolean existsTable(String tableName) {
    for (Table table : existingTables) {
      if (table.getName().equals(tableName)) {
        return true;
      }
    }
    return false;
  }

  public boolean existsColumn(String tableName, String columnName) {
    for (Table table : existingTables) {
      if (table.getName().equals(tableName)) {
        for (TableField field : table.fields()) {
          if (field.getName().equals(columnName)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public boolean existsColumnWithType(String tableName, String columnName, DataType<?> columnType) {
    for (Table table : existingTables) {
      if (table.getName().equals(tableName)) {
        for (TableField field : table.fields()) {
          if (field.getName().equals(columnName)) {
            if (field.getSqlType() == columnType.getSQLType() && field.getTypeName().equals(
                COLUMN_TYPE_NAME_SPLITTER
                    .splitToList(columnType.getTypeName()).stream()
                    .reduce((e1, e2) -> e2).get())) {
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

  public TableField getColumn(String tableName, String columnName) {
    for (Table table : existingTables) {
      if (table.getName().equals(tableName)) {
        for (TableField field : table.fields()) {
          if (field.getName().equals(columnName)) {
            return field;
          }
        }
      }
    }
    throw new IllegalArgumentException("Column " + columnName + " in table " + schemaName + "."
        + tableName + " not found");
  }

  public boolean existsIndex(String indexName) {
    for (Index index : existingIndexes) {
      if (index.getName().equals(indexName)) {
        return true;
      }
    }
    return false;
  }

  public boolean existsIndexColumn(String indexName, int position, String columnName) {
    for (Index index : existingIndexes) {
      if (index.getName().equals(indexName)) {
        for (IndexField field : index.fields()) {
          if (field.getName().equals(columnName)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public static boolean containsField(TableField existingField,
      String collection,
      TableRef tableRef,
      Iterable<MetaFieldRecord<Object>> fields,
      Iterable<MetaScalarRecord<Object>> scalars,
      TableRefFactory tableRefFactory) {

    for (MetaFieldRecord<?> field : fields) {
      if (collection.equals(field.getCollection()) && tableRef.equals(field.getTableRefValue(
          tableRefFactory)) && existingField.getName().equals(field.getIdentifier())) {
        return true;
      }
    }

    for (MetaScalarRecord<?> scalar : scalars) {
      if (collection.equals(scalar.getCollection()) && tableRef.equals(scalar.getTableRefValue(
          tableRefFactory)) && existingField.getName().equals(scalar.getIdentifier())) {
        return true;
      }
    }

    return false;
  }

  public String getDatabase() {
    return database;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public Iterable<? extends Table> getExistingTables() {
    return existingTables;
  }

  public Iterable<? extends Index> getExistingIndexes() {
    return existingIndexes;
  }

  public static class Index {

    /*
     * - TABLE_CAT String => table catalog (may be null) - TABLE_SCHEM String => table schema (may
     * be null) - TABLE_NAME String => table name - NON_UNIQUE boolean => Can index values be
     * non-unique. false when TYPE is tableIndexStatistic - INDEX_QUALIFIER String => index catalog
     * (may be null); null when TYPE is tableIndexStatistic - INDEX_NAME String => index name; null
     * when TYPE is tableIndexStatistic - TYPE short => index type: - tableIndexStatistic - this
     * identifies table statistics that are returned in conjuction with a table's index descriptions
     * - tableIndexClustered - this is a clustered index - tableIndexHashed - this is a hashed index
     * - tableIndexOther - this is some other style of index - ORDINAL_POSITION short => column
     * sequence number within index; zero when TYPE is tableIndexStatistic - COLUMN_NAME String =>
     * column name; null when TYPE is tableIndexStatistic - ASC_OR_DESC String => column sort
     * sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported;
     * null when TYPE is tableIndexStatistic - CARDINALITY long => When TYPE is tableIndexStatistic,
     * then this is the number of rows in the table; otherwise, it is the number of unique values in
     * the index. - PAGES long => When TYPE is tableIndexStatisic then this is the number of pages
     * used for the table, otherwise it is the number of pages used for the current index. -
     * FILTER_CONDITION String => Filter condition, if any. (may be null)
     */
    private final String schema;
    private final String name;
    private final boolean unique;
    private final ImmutableList<IndexField> fields;

    public Index(String schema, String name, boolean unique, ImmutableList<IndexField> fields) {
      super();
      this.schema = schema;
      this.name = name;
      this.unique = unique;
      this.fields = fields;
    }

    public String getSchema() {
      return schema;
    }

    public String getName() {
      return name;
    }

    public boolean isUnique() {
      return unique;
    }

    public Iterable<IndexField> fields() {
      return fields;
    }

    public static class ResultSetIterator implements Iterator<Index> {

      private final ResultSet resultSet;

      private String schema = null;
      private String name = null;
      private Boolean unique = null;

      private boolean isFirst = true;
      private boolean hasNext;

      public ResultSetIterator(String schemaName, String tableName, Connection connection) {
        try {
          DatabaseMetaData metaData = connection.getMetaData();
          this.resultSet = metaData.getIndexInfo(null, schemaName, tableName, false, true);
          this.hasNext = resultSet.next();
        } catch (SQLException sqlException) {
          throw new SystemException(sqlException);
        }
      }

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public Index next() {
        List<IndexField> fields = new ArrayList<>();

        try {
          do {
            String schema = resultSet.getString("TABLE_SCHEM");
            String name = resultSet.getString("INDEX_NAME");
            boolean unique = !resultSet.getBoolean("NON_UNIQUE");

            try {
              if (!this.isFirst && !this.name.equals(name)) {
                return new Index(this.schema, this.name, this.unique,
                    ImmutableList.copyOf(fields));
              }

              this.isFirst = false;

              fields.add(new IndexField(
                  resultSet.getString("COLUMN_NAME"),
                  resultSet.getInt("ORDINAL_POSITION"),
                  resultSet.getString("ASC_OR_DESC").equals("A")));
            } finally {
              this.schema = schema;
              this.name = name;
              this.unique = unique;
            }
          }
          while (resultSet.next());
        } catch (SQLException sqlException) {
          throw new SystemException(sqlException);
        }

        this.hasNext = false;

        return new Index(this.schema, this.name, this.unique,
            ImmutableList.copyOf(fields));
      }
    }

    @Override
    public String toString() {
      return "index:{" + "schema:" + schema + ", name:" + name + ", unique:" + unique + "}";
    }
  }

  public static class IndexField {

    private final String name;
    private final int position;
    private final boolean ascending;

    public IndexField(String name, int position, boolean ascending) {
      super();
      this.name = name;
      this.position = position;
      this.ascending = ascending;
    }

    public String getName() {
      return name;
    }

    public int getPosition() {
      return position;
    }

    public boolean isAscending() {
      return ascending;
    }

    @Override
    public String toString() {
      return "indexField:{" + "name:" + name + ", position:" + position
          + ", ascending:" + ascending + "}";
    }
  }

  public static class Table {

    /*
     * getTables(...): - TABLE_CAT String => table catalog (may be null) - TABLE_SCHEM String =>
     * table schema (may be null) - TABLE_NAME String => table name - TABLE_TYPE String => table
     * type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL
     * TEMPORARY", "ALIAS", "SYNONYM". - REMARKS String => explanatory comment on the table -
     * TYPE_CAT String => the types catalog (may be null) - TYPE_SCHEM String => the types schema
     * (may be null) - TYPE_NAME String => type name (may be null) - SELF_REFERENCING_COL_NAME
     * String => name of the designated "identifier" column of a typed table (may be null) -
     * REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created.
     * Values are "SYSTEM", "USER", "DERIVED". (may be null) *
     *
     * getColumns(...): - TABLE_CAT String => table catalog (may be null) - TABLE_SCHEM String =>
     * table schema (may be null) - TABLE_NAME String => table name - COLUMN_NAME String => column
     * name - DATA_TYPE int => SQL type from java.sql.Types - TYPE_NAME String => Data source
     * dependent type name, for a UDT the type name is fully qualified - COLUMN_SIZE int => column
     * size. - BUFFER_LENGTH is not used. - DECIMAL_DIGITS int => the number of fractional digits.
     * Null is returned for data types where DECIMAL_DIGITS is not applicable. - NUM_PREC_RADIX int
     * => Radix (typically either 10 or 2) - NULLABLE int => is NULL allowed. - columnNoNulls -
     * might not allow NULL values - columnNullable - definitely allows NULL values -
     * columnNullableUnknown - nullability unknown - REMARKS String => comment describing column
     * (may be null) - COLUMN_DEF String => default value for the column, which should be
     * interpreted as a string when the value is enclosed in single quotes (may be null) -
     * SQL_DATA_TYPE int => unused - SQL_DATETIME_SUB int => unused - CHAR_OCTET_LENGTH int => for
     * char types the maximum number of bytes in the column - ORDINAL_POSITION int => index of
     * column in table (starting at 1) - IS_NULLABLE String => ISO rules are used to determine the
     * nullability for a column. - YES --- if the column can include NULLs - NO --- if the column
     * cannot include NULLs - empty string --- if the nullability for the column is unknown -
     * SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if
     * DATA_TYPE isn't REF) - SCOPE_SCHEMA String => schema of table that is the scope of a
     * reference attribute (null if the DATA_TYPE isn't REF) - SCOPE_TABLE String => table name that
     * this the scope of a reference attribute (null if the DATA_TYPE isn't REF) - SOURCE_DATA_TYPE
     * short => source type of a distinct type or user-generated Ref type, SQL type from
     * java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF) - IS_AUTOINCREMENT
     * String => Indicates whether this column is auto incremented - YES --- if the column is auto
     * incremented - NO --- if the column is not auto incremented - empty string --- if it cannot be
     * determined whether the column is auto incremented - IS_GENERATEDCOLUMN String => Indicates
     * whether this is a generated column - YES --- if this a generated column - NO --- if this not
     * a generated column - empty string --- if it cannot be determined whether this is a generated
     * column
     */
    private final String schema;
    private final String name;
    private final ImmutableList<TableField> fields;

    public Table(String schema, String name, ImmutableList<TableField> fields) {
      super();
      this.schema = schema;
      this.name = name;
      this.fields = fields;
    }

    public String getSchema() {
      return schema;
    }

    public String getName() {
      return name;
    }

    public Iterable<TableField> fields() {
      return fields;
    }

    public static class ResultSetIterator implements Iterator<Table> {

      private final ResultSet tableResultSet;
      private final DatabaseMetaData metaData;

      private boolean hasNext;

      public ResultSetIterator(String schemaName, Connection connection) {
        try {
          this.metaData = connection.getMetaData();
          this.tableResultSet = metaData.getTables(null, schemaName, null, new String[]{"TABLE"});
          this.hasNext = tableResultSet.next();
        } catch (SQLException sqlException) {
          throw new SystemException(sqlException);
        }
      }

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public Table next() {
        List<TableField> fields = new ArrayList<>();

        try {
          String catalog = tableResultSet.getString("TABLE_CAT");
          String schema = tableResultSet.getString("TABLE_SCHEM");
          String name = tableResultSet.getString("TABLE_NAME");
          ResultSet columnResultSet = metaData.getColumns(catalog, schema, name, null);
          while (columnResultSet.next()) {
            fields.add(new TableField(
                columnResultSet.getString("COLUMN_NAME"),
                columnResultSet.getInt("ORDINAL_POSITION"),
                columnResultSet.getInt("DATA_TYPE"),
                columnResultSet.getString("TYPE_NAME"),
                columnResultSet.getInt("SOURCE_DATA_TYPE")));
          }

          if (!tableResultSet.next()) {
            this.hasNext = false;
          }

          return new Table(schema, name,
              ImmutableList.copyOf(fields));
        } catch (SQLException sqlException) {
          throw new SystemException(sqlException);
        }
      }
    }

    @Override
    public String toString() {
      return "table:{" + "schema:" + schema + ", name:" + name + "}";
    }
  }

  public static class TableField {

    private final String name;
    private final int position;
    private final int sqlType;
    private final String typeName;
    private final int sourceDataType;

    public TableField(String name, int position, int sqlType, String typeName, int sourceDataType) {
      super();
      this.name = name;
      this.position = position;
      this.sqlType = sqlType;
      this.typeName = typeName;
      this.sourceDataType = sourceDataType;
    }

    public String getName() {
      return name;
    }

    public int getPosition() {
      return position;
    }

    public int getSqlType() {
      return sqlType;
    }

    public String getTypeName() {
      return typeName;
    }

    public int getSourceDataType() {
      return sourceDataType;
    }

    @Override
    public String toString() {
      return "tableField:{" + "name:" + name + ", position:" + position
          + ", sqlType:" + sqlType + ", typeName:" + typeName
          + ", sourceDataType:" + sourceDataType + "}";
    }
  }
}
