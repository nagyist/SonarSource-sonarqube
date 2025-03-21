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
package org.sonar.db.schemamigration;

import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.sonar.api.utils.System2;
import org.sonar.db.DbSession;
import org.sonar.db.DbTester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaMigrationDaoIT {
  @RegisterExtension
  private final DbTester dbTester = DbTester.create(System2.INSTANCE);

  private DbSession dbSession = dbTester.getSession();
  private SchemaMigrationDao underTest = dbTester.getDbClient().schemaMigrationDao();

  @AfterEach
  void tearDown() throws Exception {
    // schema_migration is not cleared by DbTester
    try (Statement statement = dbTester.getSession().getConnection().createStatement()) {
      statement.execute("truncate table schema_migrations");
    }
  }

  @Test
  void insert_fails_with_NPE_if_argument_is_null() {
    assertThatThrownBy(() -> underTest.insert(dbSession, null))
      .isInstanceOf(NullPointerException.class)
      .hasMessage("version can't be null");
  }

  @Test
  void insert_fails_with_IAE_if_argument_is_empty() {
    assertThatThrownBy(() -> underTest.insert(dbSession, ""))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("version can't be empty");
  }

  @Test
  void getVersions_returns_an_empty_list_if_table_is_empty() {
    assertThat(underTest.selectVersions(dbSession)).isEmpty();
  }

  @Test
  void getVersions_returns_all_versions_in_table() {
    underTest.insert(dbSession, "22");
    underTest.insert(dbSession, "1");
    underTest.insert(dbSession, "3");
    dbSession.commit();

    assertThat(underTest.selectVersions(dbSession)).containsOnly(22, 1, 3);
  }
}
