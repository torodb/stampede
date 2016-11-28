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

package com.torodb.torod.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.BackendTransactionJob;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.dsl.backend.InsertBackendJob;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.internal.creation.MockSettingsImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

import com.torodb.core.dsl.backend.AddDatabaseDdlJob;
import com.torodb.core.dsl.backend.AddFieldDdlJob;
import com.torodb.core.dsl.backend.AddScalarDddlJob;
import com.torodb.core.dsl.backend.AddCollectionDdlJob;
import com.torodb.core.dsl.backend.AddDocPartDdlJob;

/**
 *
 * @author gortiz
 */
public class DefaultToBackendFunctionTest {

  private final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
  private BackendTransactionJobFactory factory;
  private ImmutableMetaCollection collection = new ImmutableMetaCollection("aColName", "aColId",
      Collections.emptyList(), Collections.emptyList());
  private MetaDatabase database = new ImmutableMetaDatabase("aDb", "aId", Collections.singletonList(
      collection));
  private DefaultToBackendFunction fun;

  public DefaultToBackendFunctionTest() {
  }

  @Before
  public void setUp() {
    factory = spy(new MockBackendConnectionJobFactory());
    fun = new DefaultToBackendFunction(factory, database, collection);
  }

  @Test
  public void testApply_noChanges() {
    MockSettings settings = new MockSettingsImpl().defaultAnswer((t) -> {
      throw new AssertionError("Method " + t.getMethod() + " was not expected to be called");
    });

    BatchMetaDocPart allCreatedDocPart = mock(BatchMetaDocPart.class, settings);
    doReturn(false)
        .when(allCreatedDocPart).isCreatedOnCurrentBatch();
    doReturn(Collections.emptyList())
        .when(allCreatedDocPart).getOnBatchModifiedMetaFields();
    doReturn(Collections.emptyList())
        .when(allCreatedDocPart).getOnBatchModifiedMetaScalars();
    DocPartData allCreatedData = mock(DocPartData.class);
    given(allCreatedData.getMetaDocPart())
        .willReturn(
            allCreatedDocPart
        );

    CollectionData collectionData = mock(CollectionData.class);

    given(collectionData.orderedDocPartData())
        .willReturn(
            Lists.<DocPartData>newArrayList(allCreatedData)
        );

    //when
    Iterable<BackendTransactionJob> result = fun.apply(collectionData);
    ArrayList<BackendTransactionJob> resultList = Lists.newArrayList(result);

    //then
    assertEquals("Expected 1 jobs to do, but " + resultList.size() + " were recived", 1, resultList
        .size());

    {
      Optional<BackendTransactionJob> insertJob = resultList.stream()
          .filter((job) -> job instanceof InsertBackendJob
              && ((InsertBackendJob) job).getDataToInsert()
                  .equals(allCreatedData))
          .findAny();
      assertTrue(insertJob.isPresent());
    }
  }

  @Test
  public void testApply_newField() {
    MockSettings settings = new MockSettingsImpl().defaultAnswer((t) -> {
      throw new AssertionError("Method " + t.getMethod() + " was not expected to be called");
    });

    BatchMetaDocPart withNewFieldsDocPart = mock(BatchMetaDocPart.class, settings);
    doReturn(false)
        .when(withNewFieldsDocPart).isCreatedOnCurrentBatch();
    doReturn(Lists.newArrayList(new ImmutableMetaField("newFieldName", "newFieldId",
        FieldType.INTEGER)))
        .when(withNewFieldsDocPart).getOnBatchModifiedMetaFields();
    doReturn(Collections.emptyList())
        .when(withNewFieldsDocPart).getOnBatchModifiedMetaScalars();
    DocPartData withNewData = mock(DocPartData.class);
    given(withNewData.getMetaDocPart())
        .willReturn(
            withNewFieldsDocPart
        );

    CollectionData collectionData = mock(CollectionData.class);

    given(collectionData.orderedDocPartData())
        .willReturn(
            Lists.<DocPartData>newArrayList(withNewData)
        );

    //when
    Iterable<BackendTransactionJob> result = fun.apply(collectionData);
    ArrayList<BackendTransactionJob> resultList = Lists.newArrayList(result);

    //then
    assertEquals("Expected 2 jobs to do, but " + resultList.size() + " were recived", 2, resultList
        .size());

    {
      Optional<BackendTransactionJob> insertJob = resultList.stream()
          .filter((job) -> job instanceof InsertBackendJob
              && ((InsertBackendJob) job).getDataToInsert()
                  .equals(withNewData))
          .findAny();
      assertTrue(insertJob.isPresent());
      Optional<BackendTransactionJob> addFieldJob = resultList.stream()
          .filter((job) -> {
            if (!(job instanceof AddFieldDdlJob)) {
              return false;
            }
            AddFieldDdlJob castedJob = (AddFieldDdlJob) job;
            return castedJob.getDocPart().equals(withNewFieldsDocPart)
                && castedJob.getField().getName().equals("newFieldName")
                && castedJob.getField().getIdentifier().equals("newFieldId");
          })
          .findAny();
      assertTrue(addFieldJob.isPresent());

      int addFieldIndex = resultList.indexOf(addFieldJob.get());
      int insertIndex = resultList.indexOf(insertJob.get());
      assert addFieldIndex >= 0;
      assert insertIndex >= 0;
      assertTrue("For a given doc part, all related add fields jobs must be executed before insert "
          + "jobs, but in this case the add field job has index " + addFieldIndex
          + " and the insert job has index " + insertIndex,
          addFieldIndex < insertIndex);
    }
  }

  @Test
  public void testApply_newScalar() {
    MockSettings settings = new MockSettingsImpl().defaultAnswer((t) -> {
      throw new AssertionError("Method " + t.getMethod() + " was not expected to be called");
    });

    BatchMetaDocPart withNewScalarDocPart = mock(BatchMetaDocPart.class, settings);
    doReturn(false)
        .when(withNewScalarDocPart).isCreatedOnCurrentBatch();
    doReturn(Collections.emptyList())
        .when(withNewScalarDocPart).getOnBatchModifiedMetaFields();
    doReturn(Lists.newArrayList(new ImmutableMetaScalar("newScalarId", FieldType.INTEGER)))
        .when(withNewScalarDocPart).getOnBatchModifiedMetaScalars();
    DocPartData withNewScalar = mock(DocPartData.class);
    given(withNewScalar.getMetaDocPart())
        .willReturn(
            withNewScalarDocPart
        );

    CollectionData collectionData = mock(CollectionData.class);

    given(collectionData.orderedDocPartData())
        .willReturn(
            Lists.<DocPartData>newArrayList(withNewScalar)
        );

    //when
    Iterable<BackendTransactionJob> result = fun.apply(collectionData);
    ArrayList<BackendTransactionJob> resultList = Lists.newArrayList(result);

    //then
    assertEquals("Expected 2 jobs to do, but " + resultList.size() + " were recived", 2, resultList
        .size());

    {
      Optional<BackendTransactionJob> insertJob = resultList.stream()
          .filter((job) -> job instanceof InsertBackendJob
              && ((InsertBackendJob) job).getDataToInsert()
                  .equals(withNewScalar))
          .findAny();
      assertTrue(insertJob.isPresent());
      Optional<BackendTransactionJob> addScalarJob = resultList.stream()
          .filter((job) -> {
            if (!(job instanceof AddScalarDddlJob)) {
              return false;
            }
            AddScalarDddlJob castedJob = (AddScalarDddlJob) job;
            return castedJob.getDocPart().equals(withNewScalarDocPart)
                && castedJob.getScalar().getIdentifier().equals("newScalarId")
                && castedJob.getScalar().getType().equals(FieldType.INTEGER);
          })
          .findAny();
      assertTrue(addScalarJob.isPresent());

      int addScalarIndex = resultList.indexOf(addScalarJob.get());
      int insertIndex = resultList.indexOf(insertJob.get());
      assert addScalarIndex >= 0;
      assert insertIndex >= 0;
      assertTrue("For a given doc part, all related add scalar jobs must be executed before insert "
          + "jobs, but in this case the add scalr job has index " + addScalarIndex
          + " and the insert job has index " + insertIndex,
          addScalarIndex < insertIndex);
    }
  }

  @Test
  public void testApply_newDocPart() {
    MockSettings settings = new MockSettingsImpl().defaultAnswer((t) -> {
      throw new AssertionError("Method " + t.getMethod() + " was not expected to be called");
    });

    BatchMetaDocPart allNewDocPart = mock(BatchMetaDocPart.class, settings);
    doReturn(true)
        .when(allNewDocPart).isCreatedOnCurrentBatch();
    doReturn(Lists.newArrayList(Lists.newArrayList(new ImmutableMetaField("newFieldName",
        "newFieldId", FieldType.BOOLEAN))).stream())
        .when(allNewDocPart).streamFields();
    doReturn(Lists.newArrayList(new ImmutableMetaScalar("newScalarId", FieldType.BOOLEAN)).stream())
        .when(allNewDocPart).streamScalars();
    DocPartData allNewData = mock(DocPartData.class);
    given(allNewData.getMetaDocPart())
        .willReturn(
            allNewDocPart
        );

    CollectionData collectionData = mock(CollectionData.class);

    given(collectionData.orderedDocPartData())
        .willReturn(
            Lists.<DocPartData>newArrayList(allNewData)
        );

    //when
    Iterable<BackendTransactionJob> result = fun.apply(collectionData);
    ArrayList<BackendTransactionJob> resultList = Lists.newArrayList(result);

    //then
    assertEquals("Expected 4 jobs to do, but " + resultList.size() + " were recived", 4, resultList
        .size());

    {
      Optional<BackendTransactionJob> insertJob = resultList.stream()
          .filter((job) -> job instanceof InsertBackendJob
              && ((InsertBackendJob) job).getDataToInsert()
                  .equals(allNewData))
          .findAny();
      assertTrue(insertJob.isPresent());
      Optional<BackendTransactionJob> addFieldJob = resultList.stream()
          .filter((job) -> {
            if (!(job instanceof AddFieldDdlJob)) {
              return false;
            }
            AddFieldDdlJob castedJob = (AddFieldDdlJob) job;
            return castedJob.getDocPart().equals(allNewDocPart)
                && castedJob.getField().getName().equals("newFieldName")
                && castedJob.getField().getIdentifier().equals("newFieldId");
          })
          .findAny();
      assertTrue(addFieldJob.isPresent());
      Optional<BackendTransactionJob> addScalarJob = resultList.stream()
          .filter((job) -> {
            if (!(job instanceof AddScalarDddlJob)) {
              return false;
            }
            AddScalarDddlJob castedJob = (AddScalarDddlJob) job;
            return castedJob.getDocPart().equals(allNewDocPart)
                && castedJob.getScalar().getIdentifier().equals("newScalarId")
                && castedJob.getScalar().getType().equals(FieldType.BOOLEAN);
          })
          .findAny();
      assertTrue(addScalarJob.isPresent());
      Optional<BackendTransactionJob> createDocPartJob = resultList.stream()
          .filter((job) -> {
            if (!(job instanceof AddDocPartDdlJob)) {
              return false;
            }
            AddDocPartDdlJob castedJob = (AddDocPartDdlJob) job;
            return castedJob.getDocPart().equals(allNewDocPart);
          })
          .findAny();
      assertTrue(createDocPartJob.isPresent());

      int createDocPartIndex = resultList.indexOf(createDocPartJob.get());
      int addFieldIndex = resultList.indexOf(addFieldJob.get());
      int addScalarIndex = resultList.indexOf(addScalarJob.get());
      int insertIndex = resultList.indexOf(insertJob.get());
      assert createDocPartIndex >= 0;
      assert addFieldIndex >= 0;
      assert addScalarIndex >= 0;
      assert insertIndex >= 0;
      assertTrue("For a given doc part, all related add fields jobs must be executed before insert "
          + "jobs, but in this case the add field job has index " + addFieldIndex
          + " and the insert job has index " + insertIndex,
          addFieldIndex < insertIndex);
      assertTrue("For a given doc part, all related add scalar jobs must be executed before insert "
          + "jobs, but in this case the add scalr job has index " + addScalarIndex
          + " and the insert job has index " + insertIndex,
          addScalarIndex < insertIndex);
      assertTrue("For a given doc part, all related create doc part jobs must be executed "
          + "before add field jobs, but in this case the create doc part job has index "
          + createDocPartIndex + " and "
          + "the add field job has index " + addFieldIndex,
          createDocPartIndex < addFieldIndex);
    }
  }

  @Test
  public void testNoBatchMetaDocPart() {
    CollectionData collectionData = mock(CollectionData.class);

    DocPartData data1 = mock(DocPartData.class);
    given(data1.getMetaDocPart())
        .willReturn(
            new WrapperMutableMetaDocPart(
                new ImmutableMetaDocPart(tableRefFactory.createRoot(), "aDocPartName"),
                (o) -> {
            }
            )
        );

    given(collectionData.orderedDocPartData())
        .willReturn(
            Collections.singleton(data1)
        );

    //when
    try {
      fun.apply(collectionData);

      //then
      fail("An exception was expected when a metadoc part which is not a " + BatchMetaDocPart.class
          + " is used");
    } catch (AssertionError | ClassCastException ex) {
    }
  }

  @Test
  public void testApplyEmpty() {
    CollectionData collectionData = mock(CollectionData.class);
    given(collectionData.orderedDocPartData())
        .willReturn(Collections.<DocPartData>emptyList());

    //when
    Iterable<BackendTransactionJob> resultIterable = fun.apply(collectionData);

    //then
    assertTrue("An empty iterator was expected", Iterables.isEmpty(resultIterable));
  }

  private static class MockBackendConnectionJobFactory implements BackendTransactionJobFactory {

    @Override
    public AddDatabaseDdlJob createAddDatabaseDdlJob(MetaDatabase db) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AddCollectionDdlJob createAddCollectionDdlJob(MetaDatabase db, MetaCollection col) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AddDocPartDdlJob createAddDocPartDdlJob(MetaDatabase db, MetaCollection col,
        MetaDocPart docPart) {
      return new AddDocPartDdlJob() {
        @Override
        public MetaDatabase getDatabase() {
          return db;
        }

        @Override
        public MetaCollection getCollection() {
          return col;
        }

        @Override
        public MetaDocPart getDocPart() {
          return docPart;
        }

        @Override
        public void execute(WriteBackendTransaction connection) throws RollbackException {
          throw new UnsupportedOperationException("Not supported yet.");
        }
      };
    }

    @Override
    public AddFieldDdlJob createAddFieldDdlJob(MetaDatabase db, MetaCollection col,
        MutableMetaDocPart docPart, MetaField field) {
      return new AddFieldDdlJob() {
        @Override
        public MetaDatabase getDatabase() {
          return db;
        }

        @Override
        public MetaCollection getCollection() {
          return col;
        }

        @Override
        public MetaDocPart getDocPart() {
          return docPart;
        }

        @Override
        public MetaField getField() {
          return field;
        }

        @Override
        public void execute(WriteBackendTransaction connection) throws RollbackException {
          throw new UnsupportedOperationException("Not supported yet.");
        }
      };
    }

    @Override
    public InsertBackendJob insert(MetaDatabase db, MetaCollection col, DocPartData data) {
      return new InsertBackendJob() {
        @Override
        public MetaDatabase getDatabase() {
          return db;
        }

        @Override
        public MetaCollection getCollection() {
          return col;
        }

        @Override
        public DocPartData getDataToInsert() {
          return data;
        }

        @Override
        public void execute(WriteBackendTransaction connection) throws RollbackException {
          throw new UnsupportedOperationException("Not supported yet.");
        }
      };
    }

    @Override
    public AddScalarDddlJob createAddScalarDdlJob(MetaDatabase db, MetaCollection col,
        MetaDocPart docPart, MetaScalar scalar) {
      return new AddScalarDddlJob() {
        @Override
        public MetaDatabase getDatabase() {
          return db;
        }

        @Override
        public MetaCollection getCollection() {
          return col;
        }

        @Override
        public MetaDocPart getDocPart() {
          return docPart;
        }

        @Override
        public MetaScalar getScalar() {
          return scalar;
        }

        @Override
        public void execute(WriteBackendTransaction connection) throws UserException {
          throw new UnsupportedOperationException("Not supported yet.");
        }
      };
    }

  }
}
