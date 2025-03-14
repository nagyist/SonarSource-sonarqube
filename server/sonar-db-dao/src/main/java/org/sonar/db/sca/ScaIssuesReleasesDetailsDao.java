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
package org.sonar.db.sca;

import java.util.List;
import org.sonar.db.Dao;
import org.sonar.db.DbSession;
import org.sonar.db.Pagination;

public class ScaIssuesReleasesDetailsDao implements Dao {

  private static ScaIssuesReleasesDetailsMapper mapper(DbSession session) {
    return session.getMapper(ScaIssuesReleasesDetailsMapper.class);
  }

  /**
   * Retrieves all issues with a specific branch UUID, no other filtering is done by this method.
   */
  public List<ScaIssueReleaseDetailsDto> selectByBranchUuid(DbSession dbSession, String branchUuid, Pagination pagination) {
    return mapper(dbSession).selectByBranchUuid(branchUuid, pagination);
  }

  /**
   * Retrieves all issues with a specific release UUID, no other filtering is done by this method.
   */
  public List<ScaIssueReleaseDetailsDto> selectByReleaseUuid(DbSession dbSession, String releaseUuid) {
    return mapper(dbSession).selectByReleaseUuid(releaseUuid);
  }

  /**
   * Counts all issues with a specific branch UUID, no other filtering is done by this method.
   */
  public int countByBranchUuid(DbSession dbSession, String branchUuid) {
    return mapper(dbSession).countByBranchUuid(branchUuid);
  }

  public List<ScaIssueReleaseDetailsDto> selectByQuery(DbSession dbSession, ScaIssuesReleasesDetailsQuery query, Pagination pagination) {
    return mapper(dbSession).selectByQuery(query, pagination);
  }

  public int countByQuery(DbSession dbSession, ScaIssuesReleasesDetailsQuery query) {
    return mapper(dbSession).countByQuery(query);
  }

  /**
   * Retrieves a single issue with a specific release.
   */
  public ScaIssueReleaseDetailsDto selectByScaIssueReleaseUuid(DbSession dbSession, String scaIssueReleaseUuid) {
    return mapper(dbSession).selectByScaIssueReleaseUuid(scaIssueReleaseUuid);
  }

}
