import json

from typing import Optional

from psycopg2 import connect
from psycopg2 import sql as pg_sql

from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsDataSourceUri,
    QgsFeature,
    QgsFeatureSink,
    QgsField,
    QgsFields,
    QgsGeometry,
    QgsProcessing,
    QgsProcessingException,
    QgsProcessingParameterDefinition,
    QgsProcessingParameterEnum,
    QgsProcessingParameterFeatureSink,
    QgsProcessingParameterProviderConnection,
    QgsProcessingParameterString,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
    QgsWkbTypes,
)
from qgis.PyQt.QtCore import QMetaType

from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name

# Schemas which can be checked: the editing session first, since the issues must be fixed there
GRAPH_CHECK_SCHEMAS = ["road_graph_editing", "road_graph"]


def split_list(value: str) -> list:
    """Split a comma separated list, and remove the empty values.

    :param value: The comma separated list, for example "G01, G02"
    :return: The list of the trimmed values, for example ["G01", "G02"]
    """
    return [v.strip() for v in (value or "").split(",") if v.strip()]


def json_value(value: object) -> Optional[str]:
    """Convert a JSON value returned by the database into a JSON string.

    :param value: The value returned by the database: a dict, a list, a string or None
    :return: The JSON string, or None
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


class CheckGraph(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    SCHEMA = "SCHEMA"
    CHECKS = "CHECKS"
    ROAD_CODES = "ROAD_CODES"
    OPTIONS = "OPTIONS"
    OUTPUT_POINTS = "OUTPUT_POINTS"
    OUTPUT_LINES = "OUTPUT_LINES"

    def name(self):
        return "check_graph"

    def displayName(self):
        return tr("Check the road graph")

    def group(self):
        return tr("Road graph checks")

    def groupId(self):
        return f"{plugin_name_normalized()}_graph_checks"

    def shortHelpString(self):
        return tr(
            "This algorithm checks the road graph, for example after an import, "
            "and returns one feature per issue found "
            "(database function road_graph.check_graph)."
            "\n"
            "\n"
            "The issues must be fixed in an editing session: check the schema of the editing session "
            "road_graph_editing, then fix the issues with the algorithm Fix the road graph issues, "
            "or manually."
            "\n"
            "\n"
            "Each issue has a check code and title, a severity (error or warning), the road code, "
            "the ids of the edges, nodes and markers concerned, some details, "
            "and the action which can fix it automatically, if any."
            "\n"
            "\n"
            "The algorithm creates two layers: a point layer locating each issue, "
            "and a line layer showing the issues for which a line is useful "
            "(edge, gap between two edges, etc.)."
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

        # Schema to check
        param = QgsProcessingParameterEnum(
            self.SCHEMA,
            tr("Schema to check"),
            options=[
                tr("Editing session (road_graph_editing)"),
                tr("Production (road_graph)"),
            ],
            defaultValue=0,
        )
        param.setHelp(
            tr(
                "The issues must be fixed in an editing session: "
                "check the production schema only to get an overview of the issues."
            )
        )
        self.addParameter(param)

        # Checks to run
        param = QgsProcessingParameterString(
            self.CHECKS,
            tr("Checks to run, separated by commas (all the checks if empty)"),
            optional=True,
        )
        param.setHelp(tr("The codes of the checks to run, for example: G01, G02"))
        self.addParameter(param)

        # Roads to check
        param = QgsProcessingParameterString(
            self.ROAD_CODES,
            tr("Road codes, separated by commas (all the roads if empty)"),
            optional=True,
        )
        param.setHelp(tr("The codes of the roads to check, for example: D40, D16"))
        self.addParameter(param)

        # Options of the checks
        param = QgsProcessingParameterString(
            self.OPTIONS,
            tr("Options of the checks (JSON)"),
            optional=True,
        )
        param.setHelp(
            tr(
                "The options of each check, with the check code as key, "
                'for example: {"G02": {"tolerance": 2}}'
            )
        )
        param.setFlags(param.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(param)

        # Output layers
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT_POINTS, tr("Road graph issues (points)"), QgsProcessing.TypeVectorPoint
            )
        )
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT_LINES, tr("Road graph issues (lines)"), QgsProcessing.TypeVectorLine
            )
        )

    @staticmethod
    def issueFields() -> QgsFields:
        """Fields describing a road graph issue.

        :return: The fields of the output layers
        """
        fields = QgsFields()
        fields.append(QgsField("id", QMetaType.Type.Int))
        fields.append(QgsField("schema_name", QMetaType.Type.QString))
        fields.append(QgsField("check_code", QMetaType.Type.QString))
        fields.append(QgsField("check_title", QMetaType.Type.QString))
        fields.append(QgsField("severity", QMetaType.Type.QString))
        fields.append(QgsField("road_code", QMetaType.Type.QString))
        fields.append(QgsField("edge_ids", QMetaType.Type.QString))
        fields.append(QgsField("node_ids", QMetaType.Type.QString))
        fields.append(QgsField("marker_ids", QMetaType.Type.QString))
        fields.append(QgsField("details", QMetaType.Type.QString))
        fields.append(QgsField("fix_action", QMetaType.Type.QString))
        fields.append(QgsField("fix_params", QMetaType.Type.QString))
        return fields

    def processAlgorithm(self, parameters, context, feedback):
        schema = GRAPH_CHECK_SCHEMAS[self.parameterAsEnum(parameters, self.SCHEMA, context)]
        checks = split_list(self.parameterAsString(parameters, self.CHECKS, context))
        road_codes = split_list(self.parameterAsString(parameters, self.ROAD_CODES, context))
        options = self.parameterAsString(parameters, self.OPTIONS, context).strip()

        # Check the options are a valid JSON object
        if options:
            try:
                if not isinstance(json.loads(options), dict):
                    raise ValueError
            except ValueError:
                raise QgsProcessingException(tr("The options of the checks must be a JSON object."))

        # Get the connection to the database
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        if not connection:
            raise QgsProcessingException(
                tr("Could not create a connection to the database with the given connection name.")
            )
        feedback.pushInfo(tr("Using connection: {}").format(connection_name))
        feedback.pushInfo(tr("Checked schema: {}").format(schema))

        # The output geometries are in the coordinate reference system of the road graph
        try:
            data = connection.executeSql("SELECT Find_SRID('road_graph', 'edges', 'geom')")
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        road_graph_crs = QgsCoordinateReferenceSystem(f"EPSG:{int(data[0][0])}")

        # Output layers
        fields = self.issueFields()
        (sink_points, dest_id_points) = self.parameterAsSink(
            parameters, self.OUTPUT_POINTS, context, fields, QgsWkbTypes.Point, road_graph_crs
        )
        if sink_points is None:
            raise QgsProcessingException(self.invalidSinkError(parameters, self.OUTPUT_POINTS))
        (sink_lines, dest_id_lines) = self.parameterAsSink(
            parameters, self.OUTPUT_LINES, context, fields, QgsWkbTypes.MultiLineString, road_graph_crs
        )
        if sink_lines is None:
            raise QgsProcessingException(self.invalidSinkError(parameters, self.OUTPUT_LINES))

        # Run the checks
        # The arrays are sent as text, with NULL for all the checks or all the roads
        sql = pg_sql.SQL("""
            SELECT
                i.id, i.check_code, i.check_title, i.severity, i.road_code,
                array_to_string(i.edge_ids, ','),
                array_to_string(i.node_ids, ','),
                array_to_string(i.marker_ids, ','),
                i.details::text, i.fix_action, i.fix_params::text,
                ST_AsText(i.geom),
                ST_AsText(ST_Multi(ST_CollectionExtract(i.geom_detail, 2)))
            FROM {schema}.check_graph(
                {checks}::text[], {road_codes}::text[], {options}::jsonb
            ) AS i
            ORDER BY i.id
        """)
        pg_conn = connect(QgsDataSourceUri(connection.uri()).connectionInfo())
        try:
            sql = sql.format(
                schema=pg_sql.Identifier(schema),
                checks=pg_sql.Literal(checks or None),
                road_codes=pg_sql.Literal(road_codes or None),
                options=pg_sql.Literal(options or None),
            ).as_string(pg_conn)
        finally:
            pg_conn.close()
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))

        # Add the issues to the output layers
        issues_count = {}
        for row in data:
            attributes = [row[0], schema] + [json_value(v) for v in row[1:11]]
            issues_count[row[1]] = issues_count.get(row[1], 0) + 1

            # Point locating the issue
            feature = QgsFeature(fields)
            feature.setAttributes(attributes)
            if row[11]:
                feature.setGeometry(QgsGeometry.fromWkt(row[11]))
            sink_points.addFeature(feature, QgsFeatureSink.FastInsert)

            # Line showing the issue, if any
            if row[12] and not QgsGeometry.fromWkt(row[12]).isEmpty():
                feature = QgsFeature(fields)
                feature.setAttributes(attributes)
                feature.setGeometry(QgsGeometry.fromWkt(row[12]))
                sink_lines.addFeature(feature, QgsFeatureSink.FastInsert)

        # Summary
        if not issues_count:
            feedback.pushInfo(tr("No issue found."))
        for check_code, count in sorted(issues_count.items()):
            feedback.pushInfo(tr("{}: {} issue(s)").format(check_code, count))

        return {self.OUTPUT_POINTS: dest_id_points, self.OUTPUT_LINES: dest_id_lines}
