package com.torodb.config.validation;

import java.net.URI;
import java.net.URISyntaxException;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class HostValidator implements ConstraintValidator<Host, String> {

	@Override
	public void initialize(Host constraintAnnotation) {
	}

	@Override
	public boolean isValid(String value, ConstraintValidatorContext context) {
		if (value != null) {
			try {
				return new URI("my://userinfo@" + value + ":80").getHost() != null;
			} catch (URISyntaxException e) {
				return false;
			}
		}
		
		return true;
	}
}
