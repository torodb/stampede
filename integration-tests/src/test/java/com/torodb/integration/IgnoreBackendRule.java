package com.torodb.integration;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class IgnoreBackendRule implements TestRule {

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                IgnoreBackend ignoreBackend = description.getAnnotation(IgnoreBackend.class);
                
                if (ignoreBackend != null && 
                        ignoreBackend.value() != null && 
                        ignoreBackend.value().length > 0) {
                    Backend currentBackend = IntegrationTestEnvironment
                            .CURRENT_INTEGRATION_TEST_ENVIRONMENT.getBackend();
                    for (Backend backendToIgnore : ignoreBackend.value()) {
                        if (backendToIgnore == currentBackend) {
                            return;
                        }
                    }
                }
                base.evaluate();
            }
        };
    }
    
}
