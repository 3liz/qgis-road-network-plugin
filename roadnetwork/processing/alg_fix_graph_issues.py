import json

from psycopg2 import connect
from psycopg2 import sql as pg_sql

from qgis.core import (
    QgsDataSourceUri,
    QgsProcessing,
    QgsProcessingException,
    QgsProcessingOutputNumber,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterProviderConnection,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
)

from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name

# The issues can only be fixed in the editing session
EDITING_SCHEMA = "road_graph_editing"

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
            if not feature["fix_action"]:
                manual_count += 1
                continue
            issues.append(
                (feature["check_code"], feature["id"], feature["fix_action"], feature["fix_params"] or "{}")
            )
        issues.sort(key=lambda issue: (issue[0], issue[1]))
        if manual_count:
            feedback.pushInfo(tr("{} issue(s) must be fixed manually.").format(manual_count))

        # Fix each issue
        counts = {"success": 0, "skipped": 0, "error": 0}
        pg_conn = connect(QgsDataSourceUri(connection.uri()).connectionInfo())
        try:
            for index, (check_code, issue_id, fix_action, fix_params) in enumerate(issues):
                if feedback.isCanceled():
                    break
                feedback.setProgress(int(100 * index / len(issues)))

                sql = pg_sql.SQL("SELECT {schema}.fix_graph_issue({action}, {params}::jsonb)::text").format(
                    schema=pg_sql.Identifier(EDITING_SCHEMA),
                    action=pg_sql.Literal(fix_action),
                    params=pg_sql.Literal(fix_params),
                )
                try:
                    data = connection.executeSql(sql.as_string(pg_conn))
                    result = json.loads(data[0][0])
                except (QgsProviderConnectionException, ValueError, IndexError) as e:
                    result = {"status": "error", "message": str(e)}

                status = result.get("status", "error")
                counts[status if status in counts else "error"] += 1
                message = tr("{} - issue {} - {}: {} {}").format(
                    check_code, issue_id, fix_action, status, result.get("message", "")
                )
                if status == "error":
                    feedback.reportError(message)
                else:
                    feedback.pushInfo(message)
        finally:
            pg_conn.close()

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
