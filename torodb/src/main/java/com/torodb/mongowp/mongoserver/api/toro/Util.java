
package com.torodb.mongowp.mongoserver.api.toro;

import com.eightkdata.nettybson.api.BSONDocument;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.translator.QueryCriteriaTranslator;
import com.torodb.translator.QueryEncapsulation;
import com.torodb.translator.QueryModifier;
import com.torodb.translator.QuerySortOrder;
import org.bson.BSONObject;

/**
 *
 */
class Util {
    private static final QueryCriteriaTranslator QUERY_CRITERIA_TRANSLATOR = new QueryCriteriaTranslator();
    

    static QueryCriteria translateQuery(BSONDocument document) throws UserToroException {
        for (String key : document.getKeys()) {
    		if (QueryModifier.getByKey(key) != null || QuerySortOrder.getByKey(key) != null) {
    			throw new UserToroException("Modifier " + key + " not supported");
    		}
    	}
    	BSONObject query = ((MongoBSONDocument) document).getBSONObject();
    	for (String key : query.keySet()) {
    		if (QueryEncapsulation.getByKey(key) != null) {
    			Object queryObject = query.get(key);
    			if (queryObject != null && queryObject instanceof BSONObject) {
    				query = (BSONObject) queryObject;
    				break;
    			}
    		}
    	}
    	return QUERY_CRITERIA_TRANSLATOR.translate(query);
    }
}
