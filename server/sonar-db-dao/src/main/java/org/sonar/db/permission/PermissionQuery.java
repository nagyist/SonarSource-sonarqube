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
package org.sonar.db.permission;

import java.util.Locale;
import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.sonar.db.WildcardPosition;
import org.sonar.db.component.ComponentDto;
import org.sonar.db.entity.EntityDto;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.sonar.api.utils.Paging.offset;
import static org.sonar.db.DaoUtils.buildLikeValue;

/**
 * Query used to get users and groups permissions
 */
@Immutable
public class PermissionQuery {
  public static final int RESULTS_MAX_SIZE = 100;
  public static final int SEARCH_QUERY_MIN_LENGTH = 3;
  public static final int DEFAULT_PAGE_SIZE = 20;
  public static final int DEFAULT_PAGE_INDEX = 1;

  // filter: return only the users or groups who have this permission
  private final String permission;
  // filter on project, else filter org permissions
  private final String entityUuid;

  // filter on login, email or name of users or groups
  private final String searchQuery;
  private final String searchQueryToSql;
  private final String searchQueryToSqlLowercase;

  // filter users or groups who have at least one permission. It does make
  // sense when the filter "permission" is set.
  private final boolean withAtLeastOnePermission;

  private final int pageSize;
  private final int pageIndex;
  private final int pageOffset;

  private PermissionQuery(Builder builder) {
    this.permission = builder.permission;
    this.withAtLeastOnePermission = builder.withAtLeastOnePermission;
    this.entityUuid = builder.entityUuid;
    this.searchQuery = builder.searchQuery;
    this.searchQueryToSql = builder.searchQuery == null ? null : buildLikeValue(builder.searchQuery, WildcardPosition.BEFORE_AND_AFTER);
    this.searchQueryToSqlLowercase = searchQueryToSql == null ? null : searchQueryToSql.toLowerCase(Locale.ENGLISH);
    this.pageSize = builder.pageSize;
    this.pageIndex = builder.pageIndex;
    this.pageOffset = offset(builder.pageIndex, builder.pageSize);
  }

  @CheckForNull
  public String getPermission() {
    return permission;
  }

  public boolean withAtLeastOnePermission() {
    return withAtLeastOnePermission;
  }

  @CheckForNull
  public String getEntityUuid() {
    return entityUuid;
  }

  @CheckForNull
  public String getSearchQuery() {
    return searchQuery;
  }

  @CheckForNull
  public String getSearchQueryToSql() {
    return searchQueryToSql;
  }

  @CheckForNull
  public String getSearchQueryToSqlLowercase() {
    return searchQueryToSqlLowercase;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getPageIndex() {
    return pageIndex;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String permission;
    private String entityUuid;
    private String searchQuery;
    private boolean withAtLeastOnePermission;

    private Integer pageIndex;
    private Integer pageSize;

    private Builder() {
      // enforce method constructor
    }

    public Builder setPermission(@Nullable String permission) {
      this.withAtLeastOnePermission = permission != null;
      this.permission = permission;
      return this;
    }

    public Builder setPermission(@Nullable ProjectPermission permission) {
      return setPermission(permission == null ? null : permission.getKey());
    }

    public Builder setEntity(ComponentDto component) {
      return setEntityUuid(component.uuid());
    }

    public Builder setEntity(EntityDto entity) {
      return setEntityUuid(entity.getUuid());
    }

    public Builder setEntityUuid(String entityUuid) {
      this.entityUuid = entityUuid;
      return this;
    }

    public Builder setSearchQuery(@Nullable String s) {
      this.searchQuery = defaultIfBlank(s, null);
      return this;
    }

    public Builder setPageIndex(@Nullable Integer i) {
      this.pageIndex = i;
      return this;
    }

    public Builder setPageSize(@Nullable Integer i) {
      this.pageSize = i;
      return this;
    }

    public Builder withAtLeastOnePermission() {
      this.withAtLeastOnePermission = true;
      return this;
    }

    public PermissionQuery build() {
      this.pageIndex = firstNonNull(pageIndex, DEFAULT_PAGE_INDEX);
      this.pageSize = firstNonNull(pageSize, DEFAULT_PAGE_SIZE);
      checkArgument(searchQuery == null || searchQuery.length() >= SEARCH_QUERY_MIN_LENGTH, "Search query should contains at least %s characters", SEARCH_QUERY_MIN_LENGTH);
      return new PermissionQuery(this);
    }
  }
}
