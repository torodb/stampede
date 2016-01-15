package com.torodb.torod.db.backends.tables;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gortiz
 */
public class SubDocTableTest {

    @Test
    public void testGetSubDocTypeId() {
        try {
            int result = SubDocTable.getSubDocTypeId("invalid");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("invalid");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("t_invalid");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("t_i");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("t_-1");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("t_-53462");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("t_+1");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("t_+53462");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        try {
            int result = SubDocTable.getSubDocTypeId("t_-534634583046384639458349538459345839458239253823948239428342");
            Assert.fail("An exception was expected, but " + result + " was recived");
        } catch (NumberFormatException ex) {
        }

        Assert.assertEquals(0, SubDocTable.getSubDocTypeId("t_0"));
        Assert.assertEquals(1, SubDocTable.getSubDocTypeId("t_1"));
        Assert.assertEquals(2, SubDocTable.getSubDocTypeId("t_2"));
        Assert.assertEquals(3, SubDocTable.getSubDocTypeId("t_3"));
        Assert.assertEquals(4, SubDocTable.getSubDocTypeId("t_4"));
        Assert.assertEquals(5, SubDocTable.getSubDocTypeId("t_5"));
        Assert.assertEquals(6, SubDocTable.getSubDocTypeId("t_6"));
        Assert.assertEquals(7, SubDocTable.getSubDocTypeId("t_7"));
        Assert.assertEquals(8, SubDocTable.getSubDocTypeId("t_8"));
        Assert.assertEquals(9, SubDocTable.getSubDocTypeId("t_9"));

        Assert.assertEquals(13452345, SubDocTable.getSubDocTypeId("t_13452345"));
        Assert.assertEquals(Integer.MAX_VALUE, SubDocTable.getSubDocTypeId("t_" + Integer.MAX_VALUE));


    }

}
