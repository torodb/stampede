package com.torodb.config.validation;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;

@Target({ FIELD, METHOD, PARAMETER, ANNOTATION_TYPE })
@Retention(RUNTIME)
@Constraint(validatedBy = NoDuplicatedReplNameValidator.class)
@ReportAsSingleViolation
public @interface NoDuplicatedReplName {
	String message() default "{com.torodb.config.validation.NoDuplicatedReplName.message}";

	Class<?>[] groups() default {};

	Class<? extends Payload>[] payload() default {};
}
