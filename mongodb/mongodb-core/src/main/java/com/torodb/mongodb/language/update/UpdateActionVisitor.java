/*
 * ToroDB - ToroDB-poc: MongoDB Core
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.mongodb.language.update;

/**
 *
 */
public interface UpdateActionVisitor<Result, Arg> {

    public Result visit(CompositeUpdateAction action, Arg arg);

    public Result visit(IncrementUpdateAction action, Arg arg);

    public Result visit(MoveUpdateAction action, Arg arg);

    public Result visit(MultiplyUpdateAction action, Arg arg);

    public Result visit(SetCurrentDateUpdateAction action, Arg arg);

    public Result visit(SetDocumentUpdateAction action, Arg arg);

    public Result visit(SetFieldUpdateAction action, Arg arg);

    public Result visit(UnsetFieldUpdateAction action, Arg arg);

}
