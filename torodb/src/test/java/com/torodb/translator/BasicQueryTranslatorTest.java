/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */


package com.torodb.translator;

import com.torodb.translator.BasicQueryTranslator;
import com.mongodb.util.JSON;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.translator.BasicQueryTranslator;
import org.bson.BSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 */
public class BasicQueryTranslatorTest {
    
    private static BasicQueryTranslator translator = new BasicQueryTranslator();
    
    public BasicQueryTranslatorTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
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
    public void testTranslate() throws Exception {
        
        
        String queryString = 
                "{"
                    + "f1: {"
                        + "$elemMatch: {"
                            + "f2: {$gt: 1}, "
                            + "$and: [{f2: {$not: {$gt: 3}}}]"
                        + "}"
                    + "}"
                + "}";
        
        printQuery(queryString);
    }
    
    private void printQuery(String queryString) throws UserToroException {
        BSONObject query = (BSONObject) JSON.parse(queryString);
        
        QueryCriteria translated = translator.translate(query);
        
        System.out.println(translated);
    }
    
}
