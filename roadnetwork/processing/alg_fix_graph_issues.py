from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingException,
    QgsProcessingOutputNumber,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterProviderConnection,
    QgsProject,
    QgsProviderRegistry,
)

from ..graph_issues import (
    EDITING_SCHEMA,
    fix_status_labels,
    is_issues_table_layer,
    issue_road_codes,
    refresh_issues,
    run_stored_fix,
)
from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name


class FixGraphIssues(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    INPUT = "INPUT"
    FIXED = "FIXED"
    SKIPPED = "SKIPPED"
    ERRORS = "ERRORS"

    def name(self):
        return "fix_graph_issues"

    def displayName(self):
        return tr("Fix the road graph issues")

    def flags(self):
        # The layers of the editing session are reloaded after the fixes
        return super().flags() | QgsProcessingAlgorithm.FlagNoThreading

    def group(self):
        return tr("Road graph checks")

    def groupId(self):
        return f"{plugin_name_normalized()}_graph_checks"

    def shortHelpString(self):
        return tr(
            "This algorithm fixes the issues of the road graph found by the algorithm Check the road graph, "
            "in the editing session (database function road_graph_editing.fix_graph_issue)."
            "\n"
            "\n"
            "Use one of the layers of the issues of the editing session, added to the project "
            "by the algorithm Check the road graph. "
            "Check the option Selected features only to fix only some issues."
            "\n"
            "\n"
            "Only the issues with a fix action are fixed: the others must be fixed manually. "
            "The issues are fixed in the order of their check codes. "
            "An issue already fixed by a previous action is skipped."
            "\n"
            "\n"
            "The result of each fix is stored in the fields fix_status and fix_message of the issues. "
            "If the layers of the editing session are in editing mode, "
            "the fixes are run in their transaction: "
            "save the modifications afterwards."
            "\n"
            "\n"
            "The roads of the issues are then checked again: the fixed issues are deleted, "
            "and the new issues are added."
        )

    def initAlgorithm(self, config=None):
        # PostgreSQL connection
        project = QgsProject.instance()
        connection_name = get_connection_name(project)
        param = QgsProcessingParameterProviderConnection(
            self.CONNECTION_NAME,
            tr("PostgreSQL connection to the database"),
            "postgres",
            defaultValue=connection_name,
            optional=False,
        )
        param.setHelp(tr("The connection to the database."))
        self.addParameter(param)

        # Issues to fix
        param = QgsProcessingParameterFeatureSource(
            self.INPUT,
            tr("Road graph issues"),
            [QgsProcessing.TypeVectorAnyGeometry],
        )
        param.setHelp(
            tr("A layer of the issues of the editing session, added by the algorithm Check the road graph.")
        )
        self.addParameter(param)

        # Outputs
        self.addOutput(QgsProcessingOutputNumber(self.FIXED, tr("Number of fixed issues")))
        self.addOutput(QgsProcessingOutputNumber(self.SKIPPED, tr("Number of skipped issues")))
        self.addOutput(QgsProcessingOutputNumber(self.ERRORS, tr("Number of issues not fixed")))

    def processAlgorithm(self, parameters, context, feedback):
        source = self.parameterAsSource(parameters, self.INPUT, context)
        if source is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT))

        # The input layer must display the table of the issues of the editing session
        layer = self.parameterAsVectorLayer(parameters, self.INPUT, context)
        if not is_issues_table_layer(layer):
            raise QgsProcessingException(
                tr(
                    "The issues must be fixed in the editing session: use a layer of the issues "
                    "of the table {}.graph_issues, added by the algorithm Check the road graph."
                ).format(EDITING_SCHEMA)
            )

        # Get the connection to the database
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        if not connection:
            raise QgsProcessingException(
                tr("Could not create a connection to the database with the given connection name.")
            )
        feedback.pushInfo(tr("Using connection: {}").format(connection_name))

        # Get the issues to fix, in the order of their check codes
        issues = []
        manual_count = 0
        for feature in source.getFeatures():
            # Issue already fixed, for example with a right click
            if feature["fix_status"] in ("success", "skipped"):
                continue
            if not feature["fix_action"]:
                manual_count += 1
                continue
            issues.append(
                (feature["check_code"], feature["id"], feature["fix_action"], issue_road_codes(feature))
            )
        issues.sort(key=lambda issue: (issue[0], issue[1]))
        if manual_count:
            feedback.pushInfo(tr("{} issue(s) must be fixed manually.").format(manual_count))

        # Fix each issue, without checking again its roads: they are checked once at the end
        # If the layers of the editing session are in editing mode, the fixes are run in their transaction
        project = context.project() or QgsProject.instance()
        counts = {"success": 0, "skipped": 0, "error": 0}
        checked_road_codes = set()
        status_labels = fix_status_labels()
        for index, (check_code, issue_id, fix_action, road_codes) in enumerate(issues):
            if feedback.isCanceled():
                break
            feedback.setProgress(int(100 * index / len(issues)))

            status, message = run_stored_fix(connection_name, issue_id, project, refresh=False)
            if status not in counts:
                status = "error"
            counts[status] += 1
            if status != "error":
                checked_road_codes.update(road_codes)

            message = tr("{} - issue {} - {}: {} {}").format(
                check_code, issue_id, fix_action, status_labels[status], message
            )
            if status == "error":
                feedback.reportError(message)
            else:
                feedback.pushInfo(message)

        # Check again the roads of the fixed issues
        if checked_road_codes:
            ok, error = refresh_issues(connection_name, project, None, sorted(checked_road_codes))
            if ok:
                feedback.pushInfo(tr("{} road(s) checked again.").format(len(checked_road_codes)))
            else:
                feedback.reportError(error)

        # Summary
        feedback.pushInfo(
            tr("{} issue(s) fixed, {} skipped, {} not fixed.").format(
                counts["success"], counts["skipped"], counts["error"]
            )
        )

        return {
            self.FIXED: counts["success"],
            self.SKIPPED: counts["skipped"],
            self.ERRORS: counts["error"],
        }
