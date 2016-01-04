package com.torodb.integration.mongo.v3m0.jstests;

import static java.lang.annotation.ElementType.FIELD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

import com.torodb.integration.config.Backend;
import com.torodb.integration.config.Protocol;

@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({FIELD})
public @interface JstestMetaInfo {
	JstestType type() default JstestType.Working;
	
	Protocol[] protocols();
	
	Backend[] backends();
	
	public enum JstestType {
		Working,
		Failing,
		FalsePositive,
		NotImplemented,
		Ignored;
		
		public static <T extends Enum<?> & Jstest> JstestMetaInfo getJstestMetaInfoFor(Class<? extends T> jstestClass, String testResource, Protocol protocol, Backend backend) {
			try {
				for (Jstest test : (Jstest[]) jstestClass.getMethod("values").invoke(null)) {
					for (String testResourceFound : test.getTestResources()) {
						if (testResourceFound.equals(testResource)) {
							JstestMetaInfo jstestMetaInfo = jstestClass.getField(test.toString()).getAnnotation(JstestMetaInfo.class);
							if (contains(jstestMetaInfo.backends(), backend) && 
									contains(jstestMetaInfo.protocols(), protocol)) {
								return jstestMetaInfo;
							}
						}
					}
				}
			} catch(Exception exception) {
				throw new RuntimeException(exception);
			}

			throw new RuntimeException("Test " + testResource + " not found in " + jstestClass.getName());
		}

		private static <T extends Enum<?>> boolean contains(T[] array, T value) {
			if (array != null) {
				for (T element : array) {
					if (element == value) {
						return true;
					}
				}
			}
			
			return false;
		}
	}
}
