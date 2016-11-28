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

package com.torodb.mongodb.language.update;

import com.google.common.collect.Maps;
import com.torodb.core.exceptions.user.UpdateException;
import com.torodb.core.language.AttributeReference;

import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class CompositeUpdateAction extends UpdateAction {

  private final Map<AttributeReference, SingleFieldUpdateAction> actions;

  CompositeUpdateAction(Map<AttributeReference, SingleFieldUpdateAction> actions) {
    this.actions = Collections.unmodifiableMap(actions);
  }

  public Map<AttributeReference, SingleFieldUpdateAction> getActions() {
    return actions;
  }

  @Override
  public void apply(UpdatedToroDocumentBuilder builder) throws UpdateException {
    for (UpdateAction subAction : getActions().values()) {
      subAction.apply(builder);
    }
  }

  @Override
  public <R, A> R accept(UpdateActionVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  public static class Builder {

    private final Map<AttributeReference, SingleFieldUpdateAction> actions = Maps.newHashMap();

    /**
     *
     * @param action
     * @param override
     * @return
     * @throws IllegalArgumentException if override is false and the attribute referenced by the
     *                                  given action it is already marked to be modified by a
     *                                  previously added action
     */
    public Builder add(SingleFieldUpdateAction action, boolean override) {
      for (AttributeReference modifiedField : action.getModifiedField()) {
        UpdateAction old = actions.put(modifiedField, action);
        if (old != null && !override) {
          throw new IllegalArgumentException("There are at least two update actions that "
              + "modifies "
              + modifiedField + ": " + old + " and " + action);
        }
      }
      return this;
    }

    public CompositeUpdateAction build() {
      return new CompositeUpdateAction(actions);
    }
  }
}
