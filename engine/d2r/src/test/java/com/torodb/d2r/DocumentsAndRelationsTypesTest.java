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

package com.torodb.d2r;

import com.google.common.io.ByteSource;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.*;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.*;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.kvdocument.types.*;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.heap.*;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;


@RunWith(Parameterized.class)
public class DocumentsAndRelationsTypesTest {

  private final TableRefFactory tableRefFactory = new TableRefFactoryImpl();

  private static final String DB1 = "test1";
  private static final String COLLA = "collA";
  private static final String COLLB = "collB";

  private static final String DB2 = "test2";
  private static final String COLLC = "collC";
  private static final String COLLD = "collD";

  private static ImmutableMetaSnapshot currentView = new ImmutableMetaSnapshot.Builder()
          .put(new ImmutableMetaDatabase.Builder(DB1, DB1)
                  .put(new ImmutableMetaCollection.Builder(COLLA, COLLA).build())
                  .put(new ImmutableMetaCollection.Builder(COLLB, COLLB).build()).build())
          .put(new ImmutableMetaDatabase.Builder(DB2, DB2)
                  .put(new ImmutableMetaCollection.Builder(COLLC, COLLC).build())
                  .put(new ImmutableMetaCollection.Builder(COLLD, COLLD).build()).build())
          .build();

  private MutableMetaSnapshot mutableSnapshot;

  private D2RTranslator d2RTranslator;
  private R2DTranslator r2dTranslator;


  private DocPartData mainDocPart;
  private DocPartData secondaryDocPart;

  @Before
  public void setup() {
    MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(currentView);

    try (SnapshotStage snapshot = mvccMetainfoRepository.startSnapshotStage()) {
      mutableSnapshot = snapshot.createMutableSnapshot();
    }

    MemoryRidGenerator ridGenerator = new MemoryRidGenerator();
    IdentifierFactory identifierFactory =
            new DefaultIdentifierFactory(new MockIdentifierInterface());
    MutableMetaDatabase db = mutableSnapshot.getMetaDatabaseByName(DB1);
    d2RTranslator = new D2RTranslatorStack(tableRefFactory, identifierFactory,
            ridGenerator, db, db.getMetaCollectionByName(COLLA));
    r2dTranslator = new R2DTranslatorImpl();
  }



  @Parameterized.Parameters(name = "{index} - {0}")
  public static Collection<Object[]> data() throws Exception {

    LinkedHashMap<String, KvValue<?>> docMap = new LinkedHashMap<>();
    docMap.put("c0", KvInteger.of(30));
    docMap.put("c1", new StringKvString("Pablo"));

    Collection<Object[]> allTests = Arrays.stream(new KvValue[] {
            KvDouble.of(2.55),
            KvBoolean.FALSE,
            KvBoolean.TRUE,
            KvInteger.of(123),
            new StringKvString("ajoajo"),
            new ByteSourceKvBinary(KvBinary.KvBinarySubtype.MONGO_MD5, Byte.parseByte("1",2), ByteSource.wrap("01001".getBytes()) ),
            KvUndefined.getInstance(),
            new ByteArrayKvMongoObjectId("101010101010".getBytes()),
            new LocalDateKvDate(LocalDate.now()),
            KvNull.getInstance(),
            KvMongoRegex.of("hello", EnumSet.allOf(KvMongoRegex.Options.class)),
            KvMongoDbPointer.of("ns", new ByteArrayKvMongoObjectId("101010101110".getBytes())),
            KvMongoJavascript.of("function"),
            KvMongoJavascriptWithScope.of("function", new KvDocument.Builder().putValue("times", KvInteger.of(3)).build()),
            KvDeprecated.of("Deprecate"),
            new DefaultKvMongoTimestamp(1234,3),
            KvLong.of(82147234L),
            KvMinKey.getInstance(),
            KvMaxKey.getInstance(),
            KvDecimal128.of(912012L,912912741L),
            KvDecimal128.of(0x7800000000000000L,912912741L),
            KvDecimal128.of(0x7c00000000000000L,912912741L),
            new LongKvInstant(234123L),
            new LocalTimeKvTime(LocalTime.now()),
            new ListKvArray(Arrays.asList(new KvValue[] {
                    KvBoolean.FALSE,
                    KvInteger.of(123),
                    KvBoolean.TRUE
            })),
            new MapKvDocument(docMap)
    }).map(
            kvValue -> new Object[] {
                    kvValue.getType().getClass().getSimpleName(),
                    kvValue
            }
    ).collect(Collectors.toList());


    Stream<Class<?>> untestedKvTypes = KvTypeFinder.findAllKvTypes().filter(
            type -> allTests.stream().noneMatch(
                    toTest -> type.isAssignableFrom(((KvValue)toTest[1]).getType().getClass())
            )
    );

    String joinedTypes = untestedKvTypes.map(Class::getSimpleName).collect(Collectors.joining(", "));

    if(!joinedTypes.isEmpty())
    {
      throw new Exception(joinedTypes+ " types aren't tested");
    }

    return allTests;
  }

  private static boolean isScalar(KvType kvType) {
    return (kvType != DocumentType.INSTANCE) && !(kvType instanceof ArrayType);
  }

  private static boolean isArray(KvType kvType) {
    return kvType instanceof ArrayType;
  }

  private static boolean isDocument(KvType kvType) {
    return kvType == DocumentType.INSTANCE;
  }


  @Parameterized.Parameter(0)
  public String collName;

  @Parameterized.Parameter(1)
  public KvValue value;


  @Test
  public void test() {
    KvDocument docToInsert = new KvDocument.Builder().putValue(collName,value).build();

    d2RTranslator.translate(docToInsert);

    Iterator<DocPartData> docPartIterator = d2RTranslator.getCollectionDataAccumulator().orderedDocPartData().iterator();

    mainDocPart= docPartIterator.next();

    if(docPartIterator.hasNext())
      secondaryDocPart = docPartIterator.next();

    //DocPartResult result = null;


    if(isScalar(value.getType()))
    {
      testScalar();
      //result = createResult(mainDocPart, false);
    }

    if(isArray(value.getType()))
    {
      testArray();
      //result = createResult(secondaryDocPart, true);
    }

    if (isDocument(value.getType()))
    {
      testDocument();
      //result = createResult(secondaryDocPart, false);
    }


    /*List<ToroDocument> toroDocs = r2dTranslator.translate(Arrays.asList(new DocPartResult[]{result}).iterator());

    toroDocs.forEach(
            toroDocument -> {
              KvDocument doc = toroDocument.getRoot();

              System.out.println(doc);


            }
    );*/


  }

  private void testScalar() {
    DocPartRow row = mainDocPart.iterator().next();
    assertEquals(row.getFieldValues().iterator().next(), value);
    assertEquals(mainDocPart.rowCount(),1);
  }

  private void testArray() {
    KvArray array = ((KvArray)value);

    Iterator<DocPartRow> it = secondaryDocPart.iterator();
    int i = 0;
    while(it.hasNext())
    {

      KvValue searched = null;
      Iterator<KvValue<?>> rowIt = it.next().getScalarValues().iterator();
      while(searched== null && rowIt.hasNext())
      {
        searched=rowIt.next();
      }
      assertTrue(((KvArray) value).contains(searched));

      i++;
    }
    assertEquals(i, array.size());
  }

  private void testDocument() {
    KvDocument doc = ((KvDocument)value);

    Iterator<DocPartRow> it = secondaryDocPart.iterator();
    DocPartRow row = it.next();


    Iterator<KvValue<?>> rowIt = row.getFieldValues().iterator();
    int i=0;
    while(rowIt.hasNext())
    {
      assertEquals(((KvDocument) value).get("c" + i),rowIt.next());
      i++;
    }

    assertEquals(i, doc.size());
  }


  /*private DocPartResult createResult(DocPartData data, boolean scalar)
  {
    if(data == null)
      return null;

    MetaDocPartBuilder builder = new MetaDocPartBuilder(data.getMetaDocPart().getTableRef());

    Iterator<MetaField> mfIterator = data.orderedMetaFieldIterator();
    MetaField metaField;
    while(mfIterator.hasNext())
    {
      metaField = mfIterator.next();
      builder.addMetaField(metaField.getName(), metaField.getIdentifier(), metaField.getType());
    }

    Iterator<DocPartRow> rowIterator = data.iterator();
    DocPartRow row;

    while(rowIterator.hasNext())
    {
      row = rowIterator.next();
      Stream<KvValue<?>> stream = Lists.newArrayList(
              scalar ? row.getScalarValues() : row.getFieldValues()
      ).stream();

      List<Object> values = stream.map(KvValue::getValue).collect(Collectors.toList());

      builder.addRow(
              row.getDid(),
              row.getPid(),
              row.getRid(),
              row.getSeq(),
              new Object[]{
                      values.toArray(new Object[values.size()])
              }
              );
    }

    return builder.getResultSet();

  }
*/
  private static class KvTypeFinder{

    public static Stream<Class<?>> findAllKvTypes(){
      return find("com.torodb.kvdocument.types").stream().filter(
              (clazz) -> KvType.class.isAssignableFrom(clazz) &&
                      !bannedClasses.contains(clazz)
      );
    }

    private static final List<Class<?>> bannedClasses = Arrays.asList(new Class<?>[]{
            NonExistentType.class,
            GenericType.class
    });

      private static final char PKG_SEPARATOR = '.';

      private static final char DIR_SEPARATOR = '/';

      private static final String CLASS_FILE_SUFFIX = ".class";

      private static final String BAD_PACKAGE_ERROR = "Unable to get resources from path '%s'. Are you sure the package '%s' exists?";

      private static List<Class<?>> find(String scannedPackage) {
        String scannedPath = scannedPackage.replace(PKG_SEPARATOR, DIR_SEPARATOR);
        URL scannedUrl = Thread.currentThread().getContextClassLoader().getResource(scannedPath);
        if (scannedUrl == null) {
          throw new IllegalArgumentException(String.format(BAD_PACKAGE_ERROR, scannedPath, scannedPackage));
        }
        File scannedDir = new File(scannedUrl.getFile());
        List<Class<?>> classes = new ArrayList<Class<?>>();
        for (File file : scannedDir.listFiles()) {
          classes.addAll(find(file, scannedPackage));
        }
        return classes;
      }

      private static List<Class<?>> find(File file, String scannedPackage) {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        String resource = scannedPackage + PKG_SEPARATOR + file.getName();
        if (file.isDirectory()) {
          for (File child : file.listFiles()) {
            classes.addAll(find(child, resource));
          }
        } else if (resource.endsWith(CLASS_FILE_SUFFIX)) {
          int endIndex = resource.length() - CLASS_FILE_SUFFIX.length();
          String className = resource.substring(0, endIndex);
          try {
            classes.add(Class.forName(className));
          } catch (ClassNotFoundException ignore) {
          }
        }
        return classes;
      }


  }
}
