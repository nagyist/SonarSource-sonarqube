/*
 * SonarQube
 * Copyright (C) 2009-2023 SonarSource SA
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
package org.sonar.server.ce.queue;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.sonar.alm.client.github.AppInstallationToken;
import org.sonar.alm.client.github.GithubApplicationClient;
import org.sonar.alm.client.github.GithubGlobalSettingsValidator;
import org.sonar.alm.client.github.config.GithubAppConfiguration;
import org.sonar.alm.client.github.config.GithubAppInstallation;
import org.sonar.api.utils.System2;
import org.sonar.auth.github.GitHubSettings;
import org.sonar.ce.queue.CeQueue;
import org.sonar.ce.queue.CeQueueImpl;
import org.sonar.ce.queue.CeTaskSubmit;
import org.sonar.core.i18n.I18n;
import org.sonar.core.util.SequenceUuidFactory;
import org.sonar.db.DbSession;
import org.sonar.db.DbTester;
import org.sonar.db.alm.setting.ALM;
import org.sonar.db.alm.setting.AlmSettingDto;
import org.sonar.db.ce.CeTaskTypes;
import org.sonar.db.component.BranchDto;
import org.sonar.db.component.ComponentDto;
import org.sonar.db.component.ProjectData;
import org.sonar.db.permission.GlobalPermission;
import org.sonar.db.project.CreationMethod;
import org.sonar.db.project.ProjectDto;
import org.sonar.db.user.UserDto;
import org.sonar.server.almintegration.ws.ProjectKeyGenerator;
import org.sonar.server.almsettings.ws.DelegatingDevOpsPlatformService;
import org.sonar.server.almsettings.ws.DevOpsPlatformService;
import org.sonar.server.almsettings.ws.DevOpsProjectDescriptor;
import org.sonar.server.almsettings.ws.GitHubDevOpsPlatformService;
import org.sonar.server.component.ComponentUpdater;
import org.sonar.server.es.TestIndexers;
import org.sonar.server.exceptions.BadRequestException;
import org.sonar.server.exceptions.ForbiddenException;
import org.sonar.server.favorite.FavoriteUpdater;
import org.sonar.server.management.ManagedInstanceService;
import org.sonar.server.permission.PermissionService;
import org.sonar.server.permission.PermissionTemplateService;
import org.sonar.server.permission.PermissionUpdater;
import org.sonar.server.project.DefaultBranchNameResolver;
import org.sonar.server.project.ProjectDefaultVisibility;
import org.sonar.server.project.Visibility;
import org.sonar.server.tester.UserSessionRule;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.stream.IntStream.rangeClosed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.sonar.core.ce.CeTaskCharacteristics.BRANCH;
import static org.sonar.db.component.BranchDto.DEFAULT_MAIN_BRANCH_NAME;
import static org.sonar.db.component.ComponentTesting.newDirectory;
import static org.sonar.db.permission.GlobalPermission.PROVISION_PROJECTS;
import static org.sonar.db.permission.GlobalPermission.SCAN;

public class ReportSubmitterIT {

  private static final String PROJECT_KEY = "MY_PROJECT";
  private static final String PROJECT_UUID = "P1";
  private static final String PROJECT_NAME = "My Project";
  private static final String TASK_UUID = "TASK_1";

  @Rule
  public final UserSessionRule userSession = UserSessionRule.standalone();
  @Rule
  public final DbTester db = DbTester.create();

  private final ProjectDefaultVisibility projectDefaultVisibility = mock(ProjectDefaultVisibility.class);
  private final DefaultBranchNameResolver defaultBranchNameResolver = mock(DefaultBranchNameResolver.class);

  private final CeQueue queue = mock(CeQueueImpl.class);
  private final TestIndexers projectIndexers = new TestIndexers();
  private final PermissionTemplateService permissionTemplateService = mock(PermissionTemplateService.class);

  private final ComponentUpdater componentUpdater = new ComponentUpdater(db.getDbClient(), mock(I18n.class), mock(System2.class), permissionTemplateService,
    new FavoriteUpdater(db.getDbClient()), projectIndexers, new SequenceUuidFactory(), defaultBranchNameResolver, mock(PermissionUpdater.class), mock(PermissionService.class));
  private final BranchSupport ossEditionBranchSupport = new BranchSupport(null);

  private final GithubApplicationClient githubApplicationClient = mock();
  private final GithubGlobalSettingsValidator githubGlobalSettingsValidator = mock();
  private final GitHubSettings gitHubSettings = mock();
  private final ProjectKeyGenerator projectKeyGenerator = mock();

  private final DevOpsPlatformService devOpsPlatformService = new DelegatingDevOpsPlatformService(
    Set.of(new GitHubDevOpsPlatformService(db.getDbClient(), githubGlobalSettingsValidator,
      githubApplicationClient, projectDefaultVisibility, projectKeyGenerator, userSession, componentUpdater, gitHubSettings, null)));

  private final DevOpsPlatformService devOpsPlatformServiceSpy = spy(devOpsPlatformService);

  private final ManagedInstanceService managedInstanceService = mock();

  private final ReportSubmitter underTest = new ReportSubmitter(queue, userSession, componentUpdater, permissionTemplateService, db.getDbClient(), ossEditionBranchSupport,
    projectDefaultVisibility, devOpsPlatformServiceSpy, managedInstanceService);

  @Before
  public void before() {
    when(projectDefaultVisibility.get(any())).thenReturn(Visibility.PUBLIC);
    when(defaultBranchNameResolver.getEffectiveMainBranchName()).thenReturn(DEFAULT_MAIN_BRANCH_NAME);
  }

  @Test
  public void submit_with_characteristics_fails_with_ISE_when_no_branch_support_delegate() {
    userSession
      .addPermission(GlobalPermission.SCAN)
      .addPermission(PROVISION_PROJECTS);
    mockSuccessfulPrepareSubmitCall();
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(), any(), eq(PROJECT_KEY)))
      .thenReturn(true);
    Map<String, String> nonEmptyCharacteristics = Map.of(BRANCH, "branch1");
    InputStream reportInput = IOUtils.toInputStream("{binary}", UTF_8);

    assertThatThrownBy(() -> underTest.submit(PROJECT_KEY, PROJECT_NAME, nonEmptyCharacteristics, reportInput))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("Current edition does not support branch feature");
  }

  @Test
  public void submit_stores_report() {
    userSession
      .addPermission(GlobalPermission.SCAN)
      .addPermission(PROVISION_PROJECTS);
    mockSuccessfulPrepareSubmitCall();
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(), any(), eq(PROJECT_KEY)))
      .thenReturn(true);

    underTest.submit(PROJECT_KEY, PROJECT_NAME, emptyMap(), IOUtils.toInputStream("{binary}", UTF_8));

    verifyReportIsPersisted(TASK_UUID);
  }

  @Test
  public void submit_a_report_on_existing_project() {
    ProjectData project = db.components().insertPrivateProject();
    UserDto user = db.users().insertUser();
    userSession.logIn(user).addProjectPermission(SCAN.getKey(), project.getProjectDto())
      .addProjectBranchMapping(project.projectUuid(), project.getMainBranchComponent());
    mockSuccessfulPrepareSubmitCall();

    underTest.submit(project.projectKey(), project.getProjectDto().getName(), emptyMap(), IOUtils.toInputStream("{binary}", UTF_8));

    verifyReportIsPersisted(TASK_UUID);
    verifyNoInteractions(permissionTemplateService);
    verify(queue).submit(argThat(submit -> submit.getType().equals(CeTaskTypes.REPORT)
                                           && submit.getComponent()
                                             .filter(cpt -> cpt.getUuid().equals(project.getMainBranchComponent().uuid()) && cpt.getEntityUuid().equals(project.projectUuid()))
                                             .isPresent()
                                           && submit.getSubmitterUuid().equals(user.getUuid())
                                           && submit.getUuid().equals(TASK_UUID)));
  }

  @Test
  public void provision_project_if_does_not_exist() {
    userSession
      .addPermission(GlobalPermission.SCAN)
      .addPermission(PROVISION_PROJECTS);
    mockSuccessfulPrepareSubmitCall();
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY))).thenReturn(true);
    when(permissionTemplateService.hasDefaultTemplateWithPermissionOnProjectCreator(any(DbSession.class), any(ProjectDto.class))).thenReturn(true);

    underTest.submit(PROJECT_KEY, PROJECT_NAME, emptyMap(), IOUtils.toInputStream("{binary}", UTF_8));

    ComponentDto createdProject = db.getDbClient().componentDao().selectByKey(db.getSession(), PROJECT_KEY).get();
    ProjectDto projectDto = db.getDbClient().projectDao().selectProjectByKey(db.getSession(), PROJECT_KEY).orElseThrow();

    verifyReportIsPersisted(TASK_UUID);
    verify(queue).submit(argThat(submit -> submit.getType().equals(CeTaskTypes.REPORT)
                                           && submit.getComponent().filter(cpt -> cpt.getUuid().equals(createdProject.uuid()) && cpt.getEntityUuid().equals(projectDto.getUuid()))
                                             .isPresent()
                                           && submit.getUuid().equals(TASK_UUID)));
    assertThat(projectDto.getCreationMethod()).isEqualTo(CreationMethod.SCANNER_API);
  }

  @Test
  public void add_project_as_favorite_when_project_creator_permission_on_permission_template() {
    UserDto user = db.users().insertUser();
    userSession
      .logIn(user)
      .addPermission(GlobalPermission.SCAN)
      .addPermission(PROVISION_PROJECTS);
    mockSuccessfulPrepareSubmitCall();
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY))).thenReturn(true);
    when(permissionTemplateService.hasDefaultTemplateWithPermissionOnProjectCreator(any(DbSession.class), any(ProjectDto.class))).thenReturn(true);

    underTest.submit(PROJECT_KEY, PROJECT_NAME, emptyMap(), IOUtils.toInputStream("{binary}", UTF_8));

    ProjectDto createdProject = db.getDbClient().projectDao().selectProjectByKey(db.getSession(), PROJECT_KEY).get();

    assertThat(db.favorites().hasFavorite(createdProject, user.getUuid())).isTrue();
  }

  @Test
  public void do_no_add_favorite_when_no_project_creator_permission_on_permission_template() {
    userSession
      .addPermission(GlobalPermission.SCAN)
      .addPermission(PROVISION_PROJECTS);
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY)))
      .thenReturn(true);
    when(permissionTemplateService.hasDefaultTemplateWithPermissionOnProjectCreator(any(DbSession.class), any(ProjectDto.class))).thenReturn(false);
    mockSuccessfulPrepareSubmitCall();

    underTest.submit(PROJECT_KEY, PROJECT_NAME, emptyMap(), IOUtils.toInputStream("{binary}"));

    ProjectDto createdProject = db.getDbClient().projectDao().selectProjectByKey(db.getSession(), PROJECT_KEY).get();
    assertThat(db.favorites().hasNoFavorite(createdProject)).isTrue();
  }

  @Test
  public void do_no_add_favorite_when_already_100_favorite_projects_and_no_project_creator_permission_on_permission_template() {
    UserDto user = db.users().insertUser();
    rangeClosed(1, 100).forEach(i -> db.favorites().add(db.components().insertPrivateProject().getProjectDto(), user.getUuid(), user.getLogin()));
    userSession
      .logIn(user)
      .addPermission(GlobalPermission.SCAN)
      .addPermission(PROVISION_PROJECTS);
    mockSuccessfulPrepareSubmitCall();
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY))).thenReturn(true);
    when(permissionTemplateService.hasDefaultTemplateWithPermissionOnProjectCreator(any(DbSession.class), any(ProjectDto.class))).thenReturn(true);

    underTest.submit(PROJECT_KEY, PROJECT_NAME, emptyMap(), IOUtils.toInputStream("{binary}", UTF_8));

    ProjectDto createdProject = db.getDbClient().projectDao().selectProjectByKey(db.getSession(), PROJECT_KEY).get();
    assertThat(db.favorites().hasNoFavorite(createdProject)).isTrue();
  }

  @Test
  public void submit_whenReportIsForANewProjectWithoutDevOpsMetadata_createsLocalProject() {
    userSession.addPermission(GlobalPermission.SCAN).addPermission(PROVISION_PROJECTS);
    mockSuccessfulPrepareSubmitCall();
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY)))
      .thenReturn(true);

    underTest.submit(PROJECT_KEY, PROJECT_NAME, emptyMap(), IOUtils.toInputStream("{binary}", UTF_8));

    assertLocalProjectWasCreated();
  }

  @Test
  public void submit_whenReportIsForANewGithubProjectWithoutValidAlmSettings_throws() {
    userSession.addPermission(GlobalPermission.SCAN).addPermission(PROVISION_PROJECTS);
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY))).thenReturn(true);
    mockSuccessfulPrepareSubmitCall();

    Map<String, String> characteristics = Map.of("random", "data");
    DevOpsProjectDescriptor projectDescriptor = new DevOpsProjectDescriptor(ALM.GITHUB, "apiUrl", "orga/repo");
    when(devOpsPlatformServiceSpy.getDevOpsProjectDescriptor(characteristics)).thenReturn(Optional.of(projectDescriptor));

    assertThatIllegalArgumentException().isThrownBy(() -> underTest.submit(PROJECT_KEY, PROJECT_NAME, characteristics, IOUtils.toInputStream("{binary}", UTF_8)))
      .withMessage("The project orga/repo could not be created. It was auto-detected as a GITHUB project and no valid DevOps platform configuration were found to access apiUrl");

    assertNoProjectWasCreated();
  }

  private void assertNoProjectWasCreated() {
    assertThat(db.getDbClient().projectDao().selectAll(db.getSession())).isEmpty();
  }

  @Test
  public void submit_whenReportIsForANewProjectWithValidAlmSettingsAutoProvisioningOnAndPermOnGh_createsProjectWithBinding() {
    userSession.addPermission(GlobalPermission.SCAN).addPermission(PROVISION_PROJECTS);
    when(managedInstanceService.isInstanceExternallyManaged()).thenReturn(true);
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY))).thenReturn(true);
    mockSuccessfulPrepareSubmitCall();

    Map<String, String> characteristics = Map.of("random", "data");
    DevOpsProjectDescriptor projectDescriptor = new DevOpsProjectDescriptor(ALM.GITHUB, "apiUrl", "orga/repo");

    AlmSettingDto almSettingDto = mockInteractionsWithDevOpsPlatformServiceSpyBeforeProjectCreation(characteristics, projectDescriptor);
    when(devOpsPlatformServiceSpy.isScanAllowedUsingPermissionsFromDevopsPlatform(almSettingDto, projectDescriptor)).thenReturn(true);

    underTest.submit(PROJECT_KEY, PROJECT_NAME, characteristics, IOUtils.toInputStream("{binary}", UTF_8));

    assertProjectWasCreatedWithBinding();
  }

  @Test
  public void submit_whenReportIsForANewProjectWithProjectDescriptorAndNoValidAlmSettingsAndAutoProvisioningOn_throws() {
    userSession.addPermission(GlobalPermission.SCAN).addPermission(PROVISION_PROJECTS);
    when(managedInstanceService.isInstanceExternallyManaged()).thenReturn(true);
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY))).thenReturn(true);
    mockSuccessfulPrepareSubmitCall();

    Map<String, String> characteristics = Map.of("random", "data");
    DevOpsProjectDescriptor projectDescriptor = new DevOpsProjectDescriptor(ALM.GITHUB, "apiUrl", "orga/repo");
    doReturn(Optional.of(projectDescriptor)).when(devOpsPlatformServiceSpy).getDevOpsProjectDescriptor(characteristics);

    assertThatIllegalArgumentException()
      .isThrownBy(() -> underTest.submit(PROJECT_KEY, PROJECT_NAME, characteristics, IOUtils.toInputStream("{binary}", UTF_8)))
      .withMessage("The project orga/repo could not be created. It was auto-detected as a GITHUB project and no valid DevOps platform configuration were found to access apiUrl");

    assertNoProjectWasCreated();
  }

  @Test
  public void submit_whenReportIsForANewProjectWithoutDevOpsMetadataAndAutoProvisioningOn_shouldCreateLocalProject() {
    userSession.addPermission(GlobalPermission.SCAN).addPermission(PROVISION_PROJECTS);
    when(managedInstanceService.isInstanceExternallyManaged()).thenReturn(true);
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY))).thenReturn(true);
    mockSuccessfulPrepareSubmitCall();

    Map<String, String> characteristics = Map.of("random", "data");

    underTest.submit(PROJECT_KEY, PROJECT_NAME, characteristics, IOUtils.toInputStream("{binary}", UTF_8));
    assertLocalProjectWasCreated();
  }

  private void assertLocalProjectWasCreated() {
    ProjectDto projectDto = db.getDbClient().projectDao().selectProjectByKey(db.getSession(), PROJECT_KEY).orElseThrow();
    assertThat(projectDto.getCreationMethod()).isEqualTo(CreationMethod.SCANNER_API);
    assertThat(projectDto.getName()).isEqualTo(PROJECT_NAME);

    BranchDto branchDto = db.getDbClient().branchDao().selectByBranchKey(db.getSession(), projectDto.getUuid(), "main").orElseThrow();
    assertThat(branchDto.isMain()).isTrue();

    assertThat(db.getDbClient().projectAlmSettingDao().selectByProject(db.getSession(), projectDto.getUuid())).isEmpty();
  }

  @Test
  public void submit_whenReportIsForANewProjectWithValidAlmSettings_createsProjectWithDevOpsBinding() {
    userSession.addPermission(GlobalPermission.SCAN).addPermission(PROVISION_PROJECTS);
    when(permissionTemplateService.wouldUserHaveScanPermissionWithDefaultTemplate(any(DbSession.class), any(), eq(PROJECT_KEY))).thenReturn(true);
    mockSuccessfulPrepareSubmitCall();

    Map<String, String> characteristics = Map.of("random", "data");
    DevOpsProjectDescriptor projectDescriptor = new DevOpsProjectDescriptor(ALM.GITHUB, "apiUrl", "orga/repo");

    mockInteractionsWithDevOpsPlatformServiceSpyBeforeProjectCreation(characteristics, projectDescriptor);

    underTest.submit(PROJECT_KEY, PROJECT_NAME, characteristics, IOUtils.toInputStream("{binary}", UTF_8));

    assertProjectWasCreatedWithBinding();
  }

  private void assertProjectWasCreatedWithBinding() {
    ProjectDto projectDto = db.getDbClient().projectDao().selectProjectByKey(db.getSession(), PROJECT_KEY).orElseThrow();
    assertThat(projectDto.getCreationMethod()).isEqualTo(CreationMethod.SCANNER_API_DEVOPS_AUTO_CONFIG);
    assertThat(projectDto.getName()).isEqualTo("repoName");

    BranchDto branchDto = db.getDbClient().branchDao().selectByBranchKey(db.getSession(), projectDto.getUuid(), "defaultBranch").orElseThrow();
    assertThat(branchDto.isMain()).isTrue();

    assertThat(db.getDbClient().projectAlmSettingDao().selectByProject(db.getSession(), projectDto.getUuid())).isPresent();
  }

  private AlmSettingDto mockInteractionsWithDevOpsPlatformServiceSpyBeforeProjectCreation(Map<String, String> characteristics, DevOpsProjectDescriptor projectDescriptor) {
    doReturn(Optional.of(projectDescriptor)).when(devOpsPlatformServiceSpy).getDevOpsProjectDescriptor(characteristics);
    AlmSettingDto almSettingDto = mock(AlmSettingDto.class);
    when(almSettingDto.getAlm()).thenReturn(ALM.GITHUB);
    when(almSettingDto.getUrl()).thenReturn("https://www.toto.com");
    when(almSettingDto.getUuid()).thenReturn("TEST_GH");
    doReturn(Optional.of(almSettingDto)).when(devOpsPlatformServiceSpy).getValidAlmSettingDto(any(), eq(projectDescriptor));
    mockGithubInteractions(almSettingDto);
    return almSettingDto;
  }

  private void mockGithubInteractions(AlmSettingDto almSettingDto) {
    GithubAppConfiguration githubAppConfiguration = mock(GithubAppConfiguration.class);
    when(githubGlobalSettingsValidator.validate(almSettingDto)).thenReturn(githubAppConfiguration);
    GithubAppInstallation githubAppInstallation = mock(GithubAppInstallation.class);
    when(githubAppInstallation.installationId()).thenReturn("5435345");
    when(githubApplicationClient.getWhitelistedGithubAppInstallations(any())).thenReturn(List.of(githubAppInstallation));
    when(githubApplicationClient.createAppInstallationToken(any(), anyLong())).thenReturn(Optional.of(mock(AppInstallationToken.class)));
    when(githubApplicationClient.createAppInstallationToken(any(), anyLong())).thenReturn(Optional.of(mock(AppInstallationToken.class)));
    when(githubApplicationClient.getInstallationId(eq(githubAppConfiguration), any())).thenReturn(Optional.of(5435345L));
    GithubApplicationClient.Repository repository = mock(GithubApplicationClient.Repository.class);
    when(repository.getDefaultBranch()).thenReturn("defaultBranch");
    when(repository.getFullName()).thenReturn("orga/repoName");
    when(repository.getName()).thenReturn("repoName");
    when(githubApplicationClient.getRepository(any(), any(), any())).thenReturn(Optional.of(repository));
    when(projectKeyGenerator.generateUniqueProjectKey(repository.getFullName())).thenReturn("projectKey");
  }

  @Test
  public void user_with_scan_permission_is_allowed_to_submit_a_report_on_existing_project() {
    ComponentDto project = db.components().insertPrivateProject().getMainBranchComponent();
    userSession.addPermission(SCAN);
    mockSuccessfulPrepareSubmitCall();

    underTest.submit(project.getKey(), project.name(), emptyMap(), IOUtils.toInputStream("{binary}", UTF_8));

    verify(queue).submit(any(CeTaskSubmit.class));
  }

  @Test
  public void submit_a_report_on_existing_project_with_project_scan_permission() {
    ProjectData projectData = db.components().insertPrivateProject();
    ProjectDto project = projectData.getProjectDto();
    userSession.addProjectPermission(SCAN.getKey(), project)
      .addProjectBranchMapping(project.getUuid(), projectData.getMainBranchComponent());
    mockSuccessfulPrepareSubmitCall();

    underTest.submit(project.getKey(), project.getName(), emptyMap(), IOUtils.toInputStream("{binary}", UTF_8));

    verify(queue).submit(any(CeTaskSubmit.class));
  }

  @Test
  public void fail_if_component_is_not_a_project() {
    ComponentDto component = db.components().insertPublicPortfolio();
    userSession.logIn().addPortfolioPermission(SCAN.getKey(), component);
    mockSuccessfulPrepareSubmitCall();

    String dbKey = component.getKey();
    String name = component.name();
    Map<String, String> emptyMap = emptyMap();
    InputStream stream = IOUtils.toInputStream("{binary}", UTF_8);
    assertThatThrownBy(() -> underTest.submit(dbKey, name, emptyMap, stream))
      .isInstanceOf(BadRequestException.class)
      .hasMessage(format("Component '%s' is not a project", component.getKey()));
  }

  @Test
  public void fail_if_project_key_already_exists_as_other_component() {
    ProjectData projectData = db.components().insertPrivateProject();
    ProjectDto project = projectData.getProjectDto();
    ComponentDto dir = db.components().insertComponent(newDirectory(projectData.getMainBranchComponent(), "path"));
    userSession.logIn().addProjectPermission(SCAN.getKey(), project);
    mockSuccessfulPrepareSubmitCall();

    String dirDbKey = dir.getKey();
    String name = dir.name();
    Map<String, String> emptyMap = emptyMap();
    InputStream inputStream = IOUtils.toInputStream("{binary}", UTF_8);
    assertThatThrownBy(() -> underTest.submit(dirDbKey, name, emptyMap, inputStream))
      .isInstanceOf(BadRequestException.class)
      .extracting(throwable -> ((BadRequestException) throwable).errors())
      .asList()
      .contains(format("The project '%s' is already defined in SonarQube but as a module of project '%s'. " +
                       "If you really want to stop directly analysing project '%s', please first delete it from SonarQube and then relaunch the analysis of project '%s'.",
        dir.getKey(), project.getKey(), project.getKey(), dir.getKey()));
  }

  @Test
  public void fail_with_forbidden_exception_when_no_scan_permission() {
    Map<String, String> emptyMap = emptyMap();
    InputStream inputStream = IOUtils.toInputStream("{binary}", UTF_8);
    assertThatThrownBy(() -> underTest.submit(PROJECT_KEY, PROJECT_NAME, emptyMap, inputStream))
      .isInstanceOf(ForbiddenException.class);
  }

  @Test
  public void fail_with_forbidden_exception_on_new_project_when_only_project_scan_permission() {
    ProjectDto project = db.components().insertPrivateProject(PROJECT_UUID).getProjectDto();
    userSession.addProjectPermission(SCAN.getKey(), project);
    mockSuccessfulPrepareSubmitCall();

    Map<String, String> emptyMap = emptyMap();
    InputStream inputStream = IOUtils.toInputStream("{binary}", UTF_8);
    assertThatThrownBy(() -> underTest.submit(PROJECT_KEY, PROJECT_NAME, emptyMap, inputStream))
      .isInstanceOf(ForbiddenException.class);
  }

  private void verifyReportIsPersisted(String taskUuid) {
    assertThat(db.selectFirst("select task_uuid from ce_task_input where task_uuid='" + taskUuid + "'")).isNotNull();
  }

  private void mockSuccessfulPrepareSubmitCall() {
    when(queue.prepareSubmit()).thenReturn(new CeTaskSubmit.Builder(TASK_UUID));
  }

}
