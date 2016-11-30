/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.packaging.config.model.backend;

import com.google.common.collect.ImmutableMap;

public abstract class AbstractBackend {

  private final ImmutableMap<String, Class<? extends BackendImplementation>> backendClasses;
  private BackendImplementation backendImplementation;

  @SuppressWarnings("checkstyle:LineLength")
  public AbstractBackend(ImmutableMap<String, Class<? extends BackendImplementation>> backendClasses) {
    this.backendClasses = backendClasses;
  }

  @SuppressWarnings("checkstyle:LineLength")
  public AbstractBackend(ImmutableMap<String, Class<? extends BackendImplementation>> backendClasses,
      BackendImplementation backendImplementation) {
    this(backendClasses);

    this.backendImplementation = backendImplementation;
  }

  public boolean hasBackendImplementation(String name) {
    return backendClasses.containsKey(name);
  }

  public Class<? extends BackendImplementation> getBackendImplementationClass(String name) {
    return backendClasses.get(name);
  }

  public String getBackendImplementationName(
      Class<? extends BackendImplementation> backendImplementationClass) {
    return backendClasses.entrySet().stream()
        .filter(entry -> entry.getValue() == backendImplementationClass
            || (backendImplementationClass.getSuperclass() != null && entry.getValue()
            == backendImplementationClass.getSuperclass()))
        .findAny()
        .orElseThrow(() -> new IllegalArgumentException("AbstractBackend not found for class "
            + backendImplementationClass.getName()))
        .getKey();
  }

  public BackendImplementation getBackendImplementation() {
    return backendImplementation;
  }

  public void setBackendImplementation(BackendImplementation backendImplementation) {
    this.backendImplementation = backendImplementation;
  }

  public boolean isLike(Class<? extends BackendImplementation> backendImplementationClass) {
    return backendImplementationClass.isAssignableFrom(backendImplementation.getClass());
  }

  public boolean is(Class<? extends BackendImplementation> backendImplementationClass) {
    return backendImplementationClass == backendImplementation.getClass();
  }

  public <T extends BackendImplementation> T as(Class<? extends T> backendImplementationClass) {
    assert backendImplementationClass.isAssignableFrom(backendImplementation.getClass());

    return backendImplementationClass.cast(backendImplementation);
  }
}
