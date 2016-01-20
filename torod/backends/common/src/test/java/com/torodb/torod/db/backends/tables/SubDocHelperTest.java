package com.torodb.torod.db.backends.tables;

import com.torodb.torod.db.backends.DatabaseInterface;
import org.junit.*;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsExceptionClass;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author gortiz
 */
public class SubDocHelperTest {

    private static DatabaseInterface databaseInterface;

    public SubDocHelperTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        databaseInterface = Mockito.mock(
                DatabaseInterface.class,
                new ThrowsExceptionClass(AssertionError.class)
        );
        Mockito.doAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                return invocation.getArgumentAt(0, String.class);
            }

        }).when(databaseInterface).escapeAttributeName(Mockito.any(String.class));
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testToColumnName() {
        SubDocHelper helper = new SubDocHelper(databaseInterface);

        Assert.assertEquals("aText", helper.toColumnName("aText"));

        Assert.assertEquals("_did", helper.toColumnName("did"));
        Assert.assertEquals("__did", helper.toColumnName("_did"));
        Assert.assertEquals("___did", helper.toColumnName("__did"));
        Assert.assertEquals("____did", helper.toColumnName("___did"));

        Assert.assertEquals("_index", helper.toColumnName("index"));
        Assert.assertEquals("__index", helper.toColumnName("_index"));
        Assert.assertEquals("___index", helper.toColumnName("__index"));
        Assert.assertEquals("____index", helper.toColumnName("___index"));
    }

    @Test
    public void testToAttributeName() {
        Assert.assertEquals("aText", SubDocHelper.toAttributeName("aText"));

        Assert.assertEquals("did", SubDocHelper.toAttributeName("_did"));
        Assert.assertEquals("_did", SubDocHelper.toAttributeName("__did"));
        Assert.assertEquals("__did", SubDocHelper.toAttributeName("___did"));

        Assert.assertEquals("index", SubDocHelper.toAttributeName("_index"));
        Assert.assertEquals("_index", SubDocHelper.toAttributeName("__index"));
        Assert.assertEquals("__index", SubDocHelper.toAttributeName("___index"));
    }

}
