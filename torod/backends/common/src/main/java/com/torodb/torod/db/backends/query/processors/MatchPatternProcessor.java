
package com.torodb.torod.db.backends.query.processors;

import com.torodb.torod.core.language.querycriteria.MatchPatternQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class MatchPatternProcessor {

    public static List<ProcessedQueryCriteria> process(
            MatchPatternQueryCriteria criteria, 
            QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor) {
    
        QueryCriteria structureCriteria = Utils.getStructureQueryCriteria(criteria.getAttributeReference(), BasicType.STRING);

        return Collections.singletonList(
                new ProcessedQueryCriteria(
                        structureCriteria,
                        criteria)
        );
        
    }

}
