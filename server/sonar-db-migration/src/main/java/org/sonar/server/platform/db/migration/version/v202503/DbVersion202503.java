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
package org.sonar.server.platform.db.migration.version.v202503;

import org.sonar.server.platform.db.migration.step.MigrationStepRegistry;
import org.sonar.server.platform.db.migration.version.DbVersion;

// ignoring bad number formatting, as it's indented that we align the migration numbers to SQ versions
@SuppressWarnings("java:S3937")
public class DbVersion202503 implements DbVersion {

  /**
   * We use the start of the 10.X cycle as an opportunity to align migration numbers with the SQ version number.
   * Please follow this pattern:
   * 2025_03_000
   * 2025_03_001
   * 2025_03_002
   */
  @Override
  public void addSteps(MigrationStepRegistry registry) {
    registry
      .add(2025_03_000, "Add known_package column to SCA releases", AddKnownPackageToScaReleasesTable.class)
      .add(2025_03_001, "Populate known_package column to SCA releases", PopulateKnownPackageColumnForScaReleasesTable.class)
      .add(2025_03_002, "Update known_package on SCA release to be not nullable", UpdateKnownPackageColumnNotNullable.class);
  }
}
