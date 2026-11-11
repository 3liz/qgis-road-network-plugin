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
    ISSUES_RUN_PROPERTY,
    fix_status_labels,
    run_fix,
    set_issue_fix_result,
)
from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name

# Fields needed in the input layer, created by the algorithm Check the road graph
NEEDED_FIELDS = ("id", "schema_name", "check_code", "fix_action", "fix_params")


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
        # The issues layers of the project are updated with the result of the fixes
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
            "Use one of the layers created by the algorithm Check the road graph on the schema "
            "of the editing session. Check the option Selected features only to fix only some issues."
            "\n"
            "\n"
            "Only the issues with a fix action are fixed: the others must be fixed manually. "
            "The issues are fixed in the order of their check codes. "
            "An issue already fixed by a previous action is skipped."
            "\n"
            "\n"
            "The result of each fix is stored in the fields fix_status and fix_message of the issues layers. "
            "If the layers of the editing session are in editing mode, "
            "the fixes are run in their transaction: "
            "save the modifications afterwards."
            "\n"
            "\n"
            "Run the algorithm Check the road graph again afterwards to check the result."
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
        param.setHelp(tr("A layer created by the algorithm Check the road graph, on the editing session."))
        self.addParameter(param)

        # Outputs
        self.addOutput(QgsProcessingOutputNumber(self.FIXED, tr("Number of fixed issues")))
        self.addOutput(QgsProcessingOutputNumber(self.SKIPPED, tr("Number of skipped issues")))
        self.addOutput(QgsProcessingOutputNumber(self.ERRORS, tr("Number of issues not fixed")))

    def processAlgorithm(self, parameters, context, feedback):
        source = self.parameterAsSource(parameters, self.INPUT, context)
        if source is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT))

        # Check the input layer has been created by the algorithm Check the road graph
        missing_fields = [f for f in NEEDED_FIELDS if source.fields().indexOf(f) < 0]
        if missing_fields:
            raise QgsProcessingException(
                tr(
                    "The input layer must be created by the algorithm Check the road graph. "
                    "Missing fields: {}"
                ).format(", ".join(missing_fields))
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
            if feature["schema_name"] != EDITING_SCHEMA:
                raise QgsProcessingException(
                    tr(
                        "The issues must be fixed in the editing session: "
                        "run the algorithm Check the road graph on the schema {}."
                    ).format(EDITING_SCHEMA)
                )
            # Issue already fixed, for example with a right click
            has_status = feature.fields().indexOf("fix_status") >= 0
            if has_status and feature["fix_status"] in ("success", "skipped"):
                continue
            if not feature["fix_action"]:
                manual_count += 1
                continue
            issues.append(
                (feature["check_code"], feature["id"], feature["fix_action"], feature["fix_params"] or "{}")
            )
        issues.sort(key=lambda issue: (issue[0], issue[1]))
        if manual_count:
            feedback.pushInfo(tr("{} issue(s) must be fixed manually.").format(manual_count))

        # The layer of the issues, to store the result of the fixes in the issues layers of the same run
        layer = self.parameterAsVectorLayer(parameters, self.INPUT, context)
        run_id = layer.customProperty(ISSUES_RUN_PROPERTY) if layer else None
        project = context.project() or QgsProject.instance()

        # Fix each issue
        # If the layers of the editing session are in editing mode, the fixes are run in their transaction
        counts = {"success": 0, "skipped": 0, "error": 0}
        status_labels = fix_status_labels()
        for index, (check_code, issue_id, fix_action, fix_params) in enumerate(issues):
            if feedback.isCanceled():
                break
            feedback.setProgress(int(100 * index / len(issues)))

            status, message = run_fix(connection_name, fix_action, fix_params, project)
            if status not in counts:
                status = "error"
            counts[status] += 1
            if run_id:
                set_issue_fix_result(project, run_id, issue_id, status, message, layers=[layer])

            message = tr("{} - issue {} - {}: {} {}").format(
                check_code, issue_id, fix_action, status_labels[status], message
            )
            if status == "error":
                feedback.reportError(message)
            else:
                feedback.pushInfo(message)

        # Summary
        feedback.pushInfo(
            tr("{} issue(s) fixed, {} skipped, {} not fixed.").format(
                counts["success"], counts["skipped"], counts["error"]
            )
        )
        if counts["success"]:
            feedback.pushInfo(tr("Run the algorithm Check the road graph again to check the result."))

        return {
            self.FIXED: counts["success"],
            self.SKIPPED: counts["skipped"],
            self.ERRORS: counts["error"],
        }
