
package com.torodb.torod.mongodb.annotations;

import com.eightkdata.mongowp.client.core.MongoClient;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is used identify resources that has to be attend external
 * requests.
 * <p/>
 * There are several resources whose behaviour is different if it is used by
 * an external request (mongowp) or by a local request. For instance,
 * external calls to {@link MongoClient} must fulfil some conditions (like 
 * replication state) that internal ones do not.
 *
 * @see Local
 */
@Qualifier
@Target({ FIELD, PARAMETER, METHOD, TYPE })
@Retention(RUNTIME)
@Documented
public @interface External {

}
