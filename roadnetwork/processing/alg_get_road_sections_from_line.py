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
    QgsProcessingParameterBoolean,
    QgsProcessingParameterDefinition,
    QgsProcessingParameterFeatureSink,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterNumber,
    QgsProcessingParameterProviderConnection,
    QgsProcessingUtils,
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


class GetRoadSectionsFromLine(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    INPUT = "INPUT"
    SNAP_LINE = "SNAP_LINE"
    DENSIFY_DISTANCE = "DENSIFY_DISTANCE"
    MAX_DISTANCE = "MAX_DISTANCE"
    ROAD_CHANGE_PENALTY = "ROAD_CHANGE_PENALTY"
    TOLERANCE = "TOLERANCE"
    OUTPUT = "OUTPUT"

    def name(self):
        return "get_road_sections_from_line"

    def displayName(self):
        return tr("Get road sections from a line (GPS track)")

    def group(self):
        return tr("Linear referencing")

    def groupId(self):
        return f"{plugin_name_normalized()}_linear_referencing"

    def shortHelpString(self):
        return tr(
            "This algorithm gets the sections of the roads followed by each line of the input layer, "
            "for example a GPS track imported from a GPX file."
            "\n"
            "\n"
            "For each line, the algorithm:"
            "\n"
            "* snaps the line on the road graph edges, without zigzags between close roads "
            "(database function road_graph.get_snapped_line_from_track). "
            "This step can be skipped if the line already follows the road graph edges,"
            "\n"
            "* gets the ordered list of the roads followed by the snapped line, "
            "with the start and end references of each road "
            "(database function road_graph.get_road_sections_from_snapped_line)."
            "\n"
            "\n"
            "The output layer contains one feature per road section, with the fields of the input line, "
            "the order of the section along the line, the road code, the start and end references "
            "(marker, abscissa and cumulative distance), the direction (downstream or upstream) "
            "and the length. "
            "Its geometry is the part of the snapped line on this road, "
            "in the coordinate reference system of the road graph."
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

        # Input layer
        param = QgsProcessingParameterFeatureSource(
            self.INPUT,
            tr("Input line layer (GPS tracks)"),
            [QgsProcessing.TypeVectorLine],
        )
        param.setHelp(tr("The lines to process, in any coordinate reference system."))
        self.addParameter(param)

        # Snap the line
        param = QgsProcessingParameterBoolean(
            self.SNAP_LINE,
            tr("Snap the lines on the road graph"),
            defaultValue=True,
        )
        param.setHelp(
            tr(
                "Uncheck it only if the lines already follow the road graph edges, "
                "for example the result of a previous snapping."
            )
        )
        self.addParameter(param)

        # Parameters of the function get_snapped_line_from_track
        param = QgsProcessingParameterNumber(
            self.DENSIFY_DISTANCE,
            tr("Snapping - Distance between the points added to the line (m)"),
            type=QgsProcessingParameterNumber.Double,
            defaultValue=2,
            minValue=0.1,
        )
        param.setHelp(
            tr("The line is densified with a point every given distance, then the points are snapped.")
        )
        param.setFlags(param.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(param)

        param = QgsProcessingParameterNumber(
            self.MAX_DISTANCE,
            tr("Snapping - Maximum distance between the line and the roads (m)"),
            type=QgsProcessingParameterNumber.Double,
            defaultValue=25,
            minValue=0.1,
        )
        param.setHelp(tr("The points of the line further than this distance from any road are ignored."))
        param.setFlags(param.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(param)

        param = QgsProcessingParameterNumber(
            self.ROAD_CHANGE_PENALTY,
            tr("Snapping - Penalty applied each time the road changes"),
            type=QgsProcessingParameterNumber.Double,
            defaultValue=100,
            minValue=0,
        )
        param.setHelp(tr("Increase it if the snapped line still has zigzags between close roads."))
        param.setFlags(param.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(param)

        # Parameter of the function get_road_sections_from_snapped_line
        param = QgsProcessingParameterNumber(
            self.TOLERANCE,
            tr("Sections - Maximum distance between the snapped line and the edges (m)"),
            type=QgsProcessingParameterNumber.Double,
            defaultValue=1,
            minValue=0.01,
        )
        param.setHelp(tr("Tolerance used to find the road of each part of the snapped line."))
        param.setFlags(param.flags() | QgsProcessingParameterDefinition.FlagAdvanced)
        self.addParameter(param)

        # Output layer
        self.addParameter(
            QgsProcessingParameterFeatureSink(self.OUTPUT, tr("Road sections"), QgsProcessing.TypeVectorLine)
        )

    @staticmethod
    def sectionFields() -> QgsFields:
        """Fields describing a road section."""
        fields = QgsFields()
        fields.append(QgsField("road_order", QMetaType.Type.Int))
        fields.append(QgsField("road_code", QMetaType.Type.QString))
        fields.append(QgsField("start_marker", QMetaType.Type.Int))
        fields.append(QgsField("start_abscissa", QMetaType.Type.Double))
        fields.append(QgsField("start_cumulative", QMetaType.Type.Double))
        fields.append(QgsField("end_marker", QMetaType.Type.Int))
        fields.append(QgsField("end_abscissa", QMetaType.Type.Double))
        fields.append(QgsField("end_cumulative", QMetaType.Type.Double))
        fields.append(QgsField("direction", QMetaType.Type.QString))
        fields.append(QgsField("length", QMetaType.Type.Double))
        return fields

    def processAlgorithm(self, parameters, context, feedback):
        source = self.parameterAsSource(parameters, self.INPUT, context)
        if source is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT))

        snap_line = self.parameterAsBoolean(parameters, self.SNAP_LINE, context)
        densify_distance = self.parameterAsDouble(parameters, self.DENSIFY_DISTANCE, context)
        max_distance = self.parameterAsDouble(parameters, self.MAX_DISTANCE, context)
        road_change_penalty = self.parameterAsDouble(parameters, self.ROAD_CHANGE_PENALTY, context)
        tolerance = self.parameterAsDouble(parameters, self.TOLERANCE, context)

        # Get the connection to the database
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        if not connection:
            raise QgsProcessingException(
                tr("Could not create a connection to the database with the given connection name.")
            )
        feedback.pushInfo(tr("Using connection: {}").format(connection_name))

        # The output geometries are in the coordinate reference system of the road graph
        try:
            data = connection.executeSql("SELECT Find_SRID('road_graph', 'edges', 'geom')")
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        road_graph_srid = int(data[0][0])
        road_graph_crs = QgsCoordinateReferenceSystem(f"EPSG:{road_graph_srid}")

        # The input geometries are sent with the SRID of their layer:
        # the database functions reproject them if needed.
        # A geometry without SRID is considered to be in the road graph projection
        source_srid = source.sourceCrs().postgisSrid()
        if source_srid <= 0:
            feedback.pushWarning(
                tr(
                    "The coordinate reference system of the input layer has no EPSG code: "
                    "the lines are considered to be in the coordinate reference system of the road graph."
                )
            )
            source_srid = 0

        # Output fields: the fields of the input layer, then the fields of the section
        fields = QgsProcessingUtils.combineFields(source.fields(), self.sectionFields())
        (sink, dest_id) = self.parameterAsSink(
            parameters,
            self.OUTPUT,
            context,
            fields,
            QgsWkbTypes.LineString,
            road_graph_crs,
        )
        if sink is None:
            raise QgsProcessingException(self.invalidSinkError(parameters, self.OUTPUT))

        pg_conn = connect(QgsDataSourceUri(connection.uri()).connectionInfo())

        feature_count = source.featureCount()
        sections_count = 0
        lines_without_section = 0
        try:
            for index, feature in enumerate(source.getFeatures()):
                if feedback.isCanceled():
                    break
                feedback.setProgress(int(100 * index / feature_count) if feature_count else 0)

                geometry = feature.geometry()
                if geometry.isNull() or geometry.isEmpty():
                    lines_without_section += 1
                    continue

                # Snap the line if needed, then get the sections
                sql = pg_sql.SQL("""
                    WITH
                    line AS (
                        SELECT ST_SetSRID(ST_GeomFromText({wkt}), {srid}) AS geom
                    ),
                    snapped AS (
                        SELECT
                            CASE
                                WHEN {snap_line}
                                    THEN road_graph.get_snapped_line_from_track(
                                        l.geom, {densify_distance}, {max_distance}, {road_change_penalty}
                                    )
                                ELSE l.geom
                            END AS geom
                        FROM line AS l
                    )
                    SELECT
                        s.road_order, s.road_code,
                        -- Round the real values to 2 decimals:
                        -- a real converted to float8 gives values like 905.27001953125
                        s.start_marker,
                        round(s.start_abscissa::numeric, 2)::float8,
                        round(s.start_cumulative::numeric, 2)::float8,
                        s.end_marker,
                        round(s.end_abscissa::numeric, 2)::float8,
                        round(s.end_cumulative::numeric, 2)::float8,
                        s.direction,
                        round(s.length::numeric, 2)::float8,
                        ST_AsText(s.geom)
                    FROM snapped AS sn,
                    road_graph.get_road_sections_from_snapped_line(sn.geom, {tolerance}) AS s
                    ORDER BY s.road_order
                """).format(
                    wkt=pg_sql.Literal(geometry.asWkt()),
                    srid=pg_sql.Literal(source_srid),
                    snap_line=pg_sql.Literal(snap_line),
                    densify_distance=pg_sql.Literal(densify_distance),
                    max_distance=pg_sql.Literal(max_distance),
                    road_change_penalty=pg_sql.Literal(road_change_penalty),
                    tolerance=pg_sql.Literal(tolerance),
                )
                try:
                    data = connection.executeSql(sql.as_string(pg_conn))
                except QgsProviderConnectionException as e:
                    raise QgsProcessingException(str(e))

                if not data:
                    lines_without_section += 1
                    feedback.pushWarning(tr("No road section found for the feature {}").format(feature.id()))
                    continue

                for row in data:
                    output_feature = QgsFeature(fields)
                    output_feature.setAttributes(feature.attributes() + list(row[0:10]))
                    output_feature.setGeometry(QgsGeometry.fromWkt(row[10]))
                    sink.addFeature(output_feature, QgsFeatureSink.FastInsert)
                    sections_count += 1
        finally:
            pg_conn.close()

        feedback.pushInfo(tr("{} lines processed.").format(feature_count))
        feedback.pushInfo(tr("{} road sections found.").format(sections_count))
        if lines_without_section:
            feedback.pushWarning(tr("{} lines without any road section.").format(lines_without_section))

        return {self.OUTPUT: dest_id}
