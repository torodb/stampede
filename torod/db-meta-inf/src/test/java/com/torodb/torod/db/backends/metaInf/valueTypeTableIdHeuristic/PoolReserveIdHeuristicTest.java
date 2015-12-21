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

package com.torodb.torod.db.backends.metaInf.valueTypeTableIdHeuristic;

import com.torodb.torod.db.backends.metaInf.idHeuristic.PoolReserveIdHeuristic;
import org.junit.Test;

/**
 *
 */
public class PoolReserveIdHeuristicTest {

    public PoolReserveIdHeuristicTest() {
    }

    @Test
    public void testEvaluate() {
        final int pool = 5 * 3 * 2;
        //pool must be divisible by 2, 3 and 5 to make the test simpler
        assert pool % 2 == 0;
        assert pool % 3 == 0;
        
        PoolReserveIdHeuristic heuristic = new PoolReserveIdHeuristic(pool, 0.9d);
        
        check(heuristic, 0, 0, pool);
        
        check(heuristic, 0, pool, 0);
        check(heuristic, pool / 10, pool, 0);
        check(heuristic, pool / 5, pool, pool);
        check(heuristic, pool / 2, pool, pool);
        check(heuristic, pool / 3, pool, pool);
        check(heuristic, 2 * pool / 3, pool, pool);
        
        check(heuristic, pool, 0, pool + pool);
        check(heuristic, pool, pool / 2, pool + pool / 2);
        check(heuristic, pool, pool / 3, pool + 2 * pool / 3);
        check(heuristic, 10 * pool, pool, 10 * pool);
    }

    private void check(PoolReserveIdHeuristic heuristic, int used, int cached, int expected) {
        int result = heuristic.evaluate(used, cached);
        
        assert result == expected : "used = "+used+", cached = "+cached+", expected = "+expected+". Result = "+result;
        
        result = heuristic.evaluate(used, cached + result);
        assert result == 0;
    }
}
