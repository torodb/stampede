package com.torodb.config.validation;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;

@Target({ TYPE, ANNOTATION_TYPE })
@Retention(RUNTIME)
@Constraint(validatedBy = AnyPasswordValidator.class)
@ReportAsSingleViolation
public @interface AnyPassword {
	String message() default "{com.torodb.config.validation.PasswordOrToroPass.message}";

	Class<?>[] groups() default {};

	Class<? extends Payload>[] payload() default {};
}
