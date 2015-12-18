
package com.torodb.torod.mongodb.crp;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 *
 */
@Qualifier @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
public @interface Index {

}
