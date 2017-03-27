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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLogger;

@SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "This should never be serialized")
public class DecoratorLogger extends AbstractLogger {

  private static final long serialVersionUID = 42384109785783992L;

  private final ExtendedLogger decorate;

  public DecoratorLogger(String name, ExtendedLogger decorate, MessageFactory messageFactory) {
    super(name, messageFactory);
    this.decorate = decorate;
  }

  @Override
  public void logMessage(String fqcn, Level level, Marker marker, Message message, Throwable t) {
    decorate.logMessage(fqcn, level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, Message message, Throwable t) {
    return decorate.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, CharSequence message, Throwable t) {
    return decorate.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, Object message, Throwable t) {
    return decorate.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Throwable t) {
    return decorate.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message) {
    return decorate.isEnabled(level, marker, message);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object... params) {
    return decorate.isEnabled(level, marker, message, params);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0) {
    return decorate.isEnabled(level, marker, message, p0);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1) {
    return decorate.isEnabled(level, marker, message, p0, p1);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2) {
    return decorate.isEnabled(level, marker, message, p0, p1, p2);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3) {
    return decorate.isEnabled(level, marker, message, p0, p1, p2, p3);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3, Object p4) {
    return decorate.isEnabled(level, marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3, Object p4, Object p5) {
    return decorate.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3, Object p4, Object p5, Object p6) {
    return decorate.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    return decorate.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    return decorate.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    return decorate.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public Level getLevel() {
    return decorate.getLevel();
  }

}
