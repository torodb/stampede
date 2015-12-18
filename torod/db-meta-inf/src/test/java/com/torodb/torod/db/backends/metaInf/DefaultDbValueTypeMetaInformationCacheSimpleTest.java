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

package com.torodb.torod.db.backends.metaInf;

import com.torodb.torod.db.backends.metaInf.idHeuristic.LazyReserveIdHeuristic;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.executor.ExecutorFactory;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

/**
 *
 */
public class DefaultDbValueTypeMetaInformationCacheSimpleTest {

    @Test
    public void testCreateSubdocumentTypeTable_illegal_input_cases() {
        ExecutorFactory executorFactory = mock(ExecutorFactory.class);
        ReservedIdInfoFactory tableMetaInfoFactory = new DefaultTableMetaInfoFactory();

        DbBackend config = mock(DbBackend.class, new ThrowExceptionAnswer());
        Mockito.doReturn(64).when(config).getCacheSubDocTypeStripes();
        
        DefaultDbMetaInformationCache cache = new DefaultDbMetaInformationCache(
                executorFactory,
                new LazyReserveIdHeuristic(),
                tableMetaInfoFactory
        );

        try {
            cache.createSubDocTypeTable(null, "test", null);
            assert false;
        } catch (IllegalArgumentException ex) {
        }
    }
}
