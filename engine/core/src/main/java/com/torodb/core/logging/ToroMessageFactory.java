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

package com.torodb.core.logging;

import org.apache.logging.log4j.message.AbstractMessageFactory;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;

import java.util.SortedMap;


public abstract class ToroMessageFactory extends AbstractMessageFactory {

  private static final long serialVersionUID = 84288231412939123L;

  private final ParameterizedMessageFactory parameterized = new ParameterizedMessageFactory();

  protected abstract SortedMap<String, String> newContextMap();

  private ToroMessage newMessage(Message parameterizedMessage) {
    return new ToroMessage(parameterizedMessage, newContextMap());
  }

  @Override
  public Message newMessage(String message, Object... params) {
    return newMessage(
        parameterized.newMessage(message, params));
  }

  @Override
  public Message newMessage(String message, Object p0) {
    return newMessage(
        parameterized.newMessage(message, p0));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1) {
    return newMessage(
        parameterized.newMessage(message, p0, p1));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1, Object p2) {
    return newMessage(
        parameterized.newMessage(message, p0, p1, p2));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3) {
    return newMessage(
        parameterized.newMessage(message, p0, p1, p2, p3));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    return newMessage(
        parameterized.newMessage(message, p0, p1, p2, p3, p4));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4,
      Object p5) {
    return newMessage(
        parameterized.newMessage(message, p0, p1, p2, p3, p4, p5));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4,
      Object p5, Object p6) {
    return newMessage(
        parameterized.newMessage(message, p0, p1, p2, p3, p4, p5, p6));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4,
      Object p5, Object p6, Object p7) {
    return newMessage(
        parameterized.newMessage(message, p0, p1, p2, p3, p4, p5, p6, p7));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4,
      Object p5, Object p6, Object p7, Object p8) {
    return newMessage(
        parameterized.newMessage(message, p0, p1, p2, p3, p4, p5, p6, p7, p8));
  }

  @Override
  public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4,
      Object p5, Object p6, Object p7, Object p8, Object p9) {
    return newMessage(
        parameterized.newMessage(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9));
  }

  @Override
  public Message newMessage(String message) {
    return newMessage(super.newMessage(message));
  }

  @Override
  public Message newMessage(Object message) {
    return newMessage(super.newMessage(message));
  }

  @Override
  public Message newMessage(CharSequence message) {
    return newMessage(super.newMessage(message));
  }

}
