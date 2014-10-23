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

package com.torodb.torod.db.metaInf;

import com.torodb.torod.db.metaInf.idHeuristic.LazyReserveIdHeuristic;
import com.torodb.torod.core.config.TorodConfig;
import com.torodb.torod.core.executor.ExecutorFactory;
import javax.sql.DataSource;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 *
 */
public class DefaultDbValueTypeMetaInformationCacheSimpleTest {

    @Test
    public void testCreateSubdocumentTypeTable_illegal_input_cases() {
        ExecutorFactory executorFactory = mock(ExecutorFactory.class);
        ReservedIdInfoFactory tableMetaInfoFactory = new DefaultTableMetaInfoFactory();

        DefaultDbMetaInformationCache cache = new DefaultDbMetaInformationCache(
                executorFactory,
                new LazyReserveIdHeuristic(),
                tableMetaInfoFactory,
                new TorodConfig() {

                    @Override
                    public DataSource getDataSource() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public int getByJobDependencyStripes() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public int getCacheSubDocTypeStripes() {
                        return 64;
                    }

                    @Override
                    public int getBySessionStripes() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public long getDefaultCursorTimeout() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public int getSessionExecutorThreads() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }
                });

        try {
            cache.createSubDocTypeTable(null, "test", null);
            assert false;
        } catch (IllegalArgumentException ex) {
        }
    }
}
