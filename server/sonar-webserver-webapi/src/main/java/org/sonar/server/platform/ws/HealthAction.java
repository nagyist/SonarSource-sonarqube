/*
 * SonarQube
 * Copyright (C) 2009-2025 SonarSource SA
 * mailto:info AT sonarsource DOT com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.sonar.server.platform.ws;

import org.sonar.api.server.ws.Request;
import org.sonar.api.server.ws.Response;
import org.sonar.api.server.ws.WebService;
import org.sonar.server.exceptions.ForbiddenException;
import org.sonar.server.platform.NodeInformation;
import org.sonar.server.user.SystemPasscode;
import org.sonar.server.user.UserSession;
import org.sonar.server.ws.WsUtils;

public class HealthAction implements SystemWsAction {
  private final NodeInformation nodeInformation;
  private final HealthActionSupport support;
  private final SystemPasscode systemPasscode;
  private final UserSession userSession;

  public HealthAction(NodeInformation nodeInformation, HealthActionSupport support, SystemPasscode systemPasscode, UserSession userSession) {
    this.nodeInformation = nodeInformation;
    this.support = support;
    this.systemPasscode = systemPasscode;
    this.userSession = userSession;
  }

  @Override
  public void define(WebService.NewController controller) {
    support.define(controller, this);
  }

  @Override
  public void handle(Request request, Response response) throws Exception {
    if (!systemPasscode.isValid(request) && !isSystemAdmin()) {
      throw new ForbiddenException("Insufficient privileges");
    }

    if (nodeInformation.isStandalone()) {
      WsUtils.writeProtobuf(support.checkNodeHealth(), request, response);
    } else {
      WsUtils.writeProtobuf(support.checkClusterHealth(), request, response);
    }
  }

  private boolean isSystemAdmin() {
    return userSession.isSystemAdministrator();
  }

}
