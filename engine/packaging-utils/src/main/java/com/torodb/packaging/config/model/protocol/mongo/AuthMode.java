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

package com.torodb.packaging.config.model.protocol.mongo;

import com.torodb.packaging.config.annotation.Description;

public enum AuthMode {
  /**
   * Disable authentication mechanism. No authentication will be done
   */
  @Description(value = "config.mongo.authmode.disabled")
  disabled(false),
  /**
   * The client will negotiate best mechanism to authenticate. With server version 3.0 or above, the
   * driver will authenticate using the SCRAM-SHA-1 mechanism. Otherwise, the driver will
   * authenticate using the Challenge Response mechanism.
   */
  @Description(value = "config.mongo.authmode.negotiate")
  negotiate,
  /**
   * Challenge Response authentication
   */
  @Description(value = "config.mongo.authmode.cr")
  cr,
  /**
   * X.509 authentication
   */
  @Description(value = "config.mongo.authmode.x509")
  x509,
  /**
   * SCRAM-SHA-1 SASL authentication
   */
  @Description(value = "config.mongo.authmode.scram_sha1")
  scram_sha1;

  private final boolean enabled;

  private AuthMode() {
    this.enabled = true;
  }

  private AuthMode(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }
}
