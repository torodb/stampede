package com.torodb.integration.mongo.v3m0.jstests;

import org.junit.Assume;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.torodb.integration.config.Backend;
import com.torodb.integration.config.Protocol;

public class MongoJstestIgnoreRule implements MethodRule {

	public Statement apply(Statement base, FrameworkMethod method, Object target) {
		Statement result = base;
		
		if (target instanceof JstestsIT) {
			Jstest jstest = ((JstestsIT) target).getJstest();
			String testResource = ((JstestsIT) target).getTestResource();
			Backend backend = Backend.CURRENT;
			JstestMetaInfo jstestMetaInfo = jstest.getJstestMetaInfoFor(testResource, backend);
			if (Protocol.Mongo != Protocol.CURRENT || 
					jstestMetaInfo.type() != JstestMetaInfo.JstestType.Working) {
				result = new IgnoreStatement("Ignored becouse " + jstestMetaInfo.type().name());
			}
		}
		
		return result;
	}

	private static class IgnoreStatement extends Statement {
		private String cause;

		IgnoreStatement(String cause) {
			this.cause = cause;
		}

		@Override
		public void evaluate() {
			Assume.assumeTrue(cause, false);
		}
	}

}