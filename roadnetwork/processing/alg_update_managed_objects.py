import json
import typing

from psycopg2 import connect
from psycopg2 import sql as pg_sql
from psycopg2.extensions import connection as PsycopgConnection

from qgis.core import (
    QgsAbstractDatabaseProviderConnection,
    QgsDataSourceUri,
    QgsFeature,
    QgsFeatureSink,
    QgsGeometry,
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingException,
    QgsProcessingParameterEnum,
    QgsProcessingParameterFeatureSink,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterProviderConnection,
    QgsProject,
    QgsProviderConnectionException,
    QgsProviderRegistry,
    QgsVectorLayer,
    QgsWkbTypes,
)

from ..plugin_tools.i18n import tr
from ..plugin_tools.resources import plugin_name_normalized
from .base_algorithm import BaseProcessingAlgorithm
from .tools import get_connection_name


class UpdateManagedObjects(BaseProcessingAlgorithm):
    CONNECTION_NAME = "CONNECTION_NAME"
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    UPDATE_POLICY = "UPDATE_POLICY"
    UPDATE_POLICY_VALUES = (
        "update_geom",
        "update_references",
    )
    PRESERVE_START_END_POSITIONS = "PRESERVE_START_END_POSITIONS"
    PRESERVE_START_END_POSITIONS_VALUES = (
        "no",
        "yes",
    )

    def name(self):
        return "update_managed_objects"

    def displayName(self):
        return tr("Update managed objects from the road graph")

    def group(self):
        return tr("Editing")

    def groupId(self):
        return f"{plugin_name_normalized()}_editing"

    def shortHelpString(self):
        return tr(
            "This algorithm will allow to update the geometries "
            " or the references of the layer features."
            "\n"
            "You can choose whether the algorithm will update the geometries "
            "or the references of the features. "
            "\n"
            "\n"
            "The algorithm will request the database for each feature "
            "to get the updated geometry or references. "
            "\n"
            "The algorithm requires a connection to the PostgreSQL database "
            "and the input layer to have specific fields depending on "
            "its geometry type and the chosen update policy."
            "\n"
            "* For point layers, the required fields are : "
            "road_code, marker_code, abscissa, "
            "offset & side (offset and side are optional)."
            "\n"
            "* For linestring layers, the required fields are : "
            "road_code, start_marker_code, start_abscissa, "
            "end_marker_code, end_abscissa, offset and side "
            "(offset and side are optional)."
        )

    def flags(self):
        """
        Indicates that the algorithm supports in-place edits,
        which allows it to modify the input layer directly without creating a new output layer.
        """
        return QgsProcessingAlgorithm.FlagSupportsInPlaceEdits

    def initAlgorithm(self, config=None):
        """Initialize the algorithm by defining its parameters."""
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
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT,
                tr("Input vector layer"),
                [QgsProcessing.TypeVectorLine, QgsProcessing.TypeVectorPoint],
            )
        )

        # Update policy : geometry or references
        self.addParameter(
            QgsProcessingParameterEnum(
                self.UPDATE_POLICY,
                tr("Update policy"),
                options=[tr("Update geometries"), tr("Update references")],
                defaultValue=0,
                allowMultiple=False,
                optional=False,
                usesStaticStrings=False,
            )
        )

        # Update geometry : do we need to update references first ?
        self.addParameter(
            QgsProcessingParameterEnum(
                self.PRESERVE_START_END_POSITIONS,
                tr("Update geometries - Preserve start & end points when updating line geometries"),
                options=[tr("No"), tr("Yes")],
                defaultValue=0,
                allowMultiple=False,
                optional=False,
                usesStaticStrings=False,
            )
        )

        # Output layer
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT, tr("Output layer"), QgsProcessing.TypeVectorAnyGeometry
            )
        )

    def supportInPlaceEdit(self, layer: QgsVectorLayer) -> bool:
        """Determine if the algorithm can be run in place on the given layer."""
        # must be spatial
        return layer.isSpatial()

    def hasRequiredFields(
        self, fields: list, geometry_type: str, update_policy: str
    ) -> typing.Tuple[bool, str]:
        """Check if the layer has the required fields to run the algorithm."""
        # Needed attributes are different for points and lines.
        needed_fields = []
        # Point layer
        if geometry_type == "point":
            needed_fields = ["road_code", "marker_code", "abscissa"]
        # Linestring layer
        else:
            needed_fields = [
                "road_code",
                "start_marker_code",
                "start_abscissa",
                "end_marker_code",
                "end_abscissa",
            ]

        # Check if all needed fields are present in the layer
        missing_fields = []
        for field in needed_fields:
            if field not in fields:
                missing_fields.append(field)
        if missing_fields:
            # print(f"Missing fields : {missing_fields}")
            message = tr("The input layer is missing the following required fields: ")
            message += ", ".join(missing_fields)
            return False, message

        return True, tr("All required fields are present")

    def checkParameterValues(self, parameters, context):
        """Check if all required conditions are met to run the algorithm"""
        # Get source properties
        source = self.parameterAsSource(parameters, self.INPUT, context)
        fields = source.fields().names()
        geometry_type = "point"
        if QgsWkbTypes.geometryType(source.wkbType()) == QgsWkbTypes.LineGeometry:
            geometry_type = "linestring"

        # Get update policy
        update_policy_index = self.parameterAsEnum(parameters, self.UPDATE_POLICY, context)
        update_policy = self.UPDATE_POLICY_VALUES[update_policy_index]

        # Check required fields
        hasRequiredFields, message = self.hasRequiredFields(fields, geometry_type, update_policy)
        if not hasRequiredFields:
            return False, message

        return super(UpdateManagedObjects, self).checkParameterValues(parameters, context)

    def getReferencesFromLonLat(
        self,
        connection: QgsAbstractDatabaseProviderConnection,
        longitude: float,
        latitude: float,
        pg_conn: PsycopgConnection,
        road_code: str = "",
    ) -> dict:
        """
        Get the reference of a QGIS feature from the database
        """
        sql = (
            pg_sql.SQL("""
        SELECT
            road_graph.get_reference_from_point(
                ST_PointFromText('POINT({longitude} {latitude})', 2154),
                {road_code},
                False
            )::json AS ref
        ;
        """)
            .format(
                longitude=pg_sql.Literal(longitude),
                latitude=pg_sql.Literal(latitude),
                road_code=pg_sql.Literal(road_code) if road_code else pg_sql.SQL("NULL"),
            )
            .as_string(pg_conn)
        )

        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            pg_conn.close()
            raise QgsProcessingException(str(e))
        references = ""
        for a in data:
            references = a[0] if a else "{}"
        if references:
            return json.loads(references)

        return {}

    def getWktFromReferences(
        self,
        connection: QgsAbstractDatabaseProviderConnection,
        geometry_type: str,
        references: dict,
        pg_conn: PsycopgConnection,
    ) -> QgsGeometry:
        """
        Get the geometry of a feature from the database from its reference
        """
        if geometry_type == "point":
            sql = (
                pg_sql.SQL("""
            WITH get_geom AS (
                SELECT
                road_graph.get_road_point_from_reference(
                    {road_code},
                    {marker_code},
                    {abscissa},
                    {offset},
                    {side}
                )->'geom' AS geom
            )
            SELECT
            CASE
                WHEN geom = 'null'::jsonb THEN NULL
                ELSE ST_AsText(ST_GeomFromGeoJSON(geom))
            END
            FROM get_geom
            """)
                .format(
                    road_code=pg_sql.Literal(references["road_code"]),
                    marker_code=pg_sql.Literal(references["marker_code"]),
                    abscissa=pg_sql.Literal(references["abscissa"]),
                    offset=pg_sql.Literal(references["offset"]),
                    side=pg_sql.Literal(references["side"]),
                )
                .as_string(pg_conn)
            )
        else:
            # Linestrings
            # We need to invert start and end references if end is lower than start
            updated_references = {}
            for ref, value in references.items():
                updated_references[ref] = value
                # we calculate the start and end marker + abscissa values
                # we must convert the QVariant to float
                start_value = references["start_marker_code"] * 1000000 + references["start_abscissa"]
                end_value = references["end_marker_code"] * 1000000 + references["end_abscissa"]
                # we invert them if start is above end
                if start_value > end_value:
                    updated_references["start_marker_code"] = references["end_marker_code"]
                    updated_references["start_abscissa"] = references["end_abscissa"]
                    updated_references["end_marker_code"] = references["start_marker_code"]
                    updated_references["end_abscissa"] = references["start_abscissa"]

            sql = (
                pg_sql.SQL("""
            WITH get_geom AS (
                SELECT
                road_graph.get_road_substring_from_references(
                    {road_code},
                    {start_marker_code},
                    {start_abscissa},
                    {end_marker_code},
                    {end_abscissa},
                    {offset},
                    {side}
                )->'geom' AS geom
            )
            SELECT
            CASE
                WHEN geom = 'null'::jsonb THEN NULL
                ELSE ST_AsText(ST_GeomFromGeoJSON(geom))
            END
            FROM get_geom
            """)
                .format(
                    road_code=pg_sql.Literal(updated_references["road_code"]),
                    start_marker_code=pg_sql.Literal(updated_references["start_marker_code"]),
                    start_abscissa=pg_sql.Literal(updated_references["start_abscissa"]),
                    end_marker_code=pg_sql.Literal(updated_references["end_marker_code"]),
                    end_abscissa=pg_sql.Literal(updated_references["end_abscissa"]),
                    offset=pg_sql.Literal(updated_references["offset"]),
                    side=pg_sql.Literal(updated_references["side"]),
                )
                .as_string(pg_conn)
            )
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            pg_conn.close()
            raise QgsProcessingException(str(e))
        wkt = ""
        for a in data:
            wkt = a[0] if a else ""

        return wkt

    def processAlgorithm(self, parameters, context, feedback):
        # Get input parameters
        source = self.parameterAsSource(parameters, self.INPUT, context)

        # Get the source layer sink
        (sink, dest_id) = self.parameterAsSink(
            parameters,
            self.OUTPUT,
            context,
            source.fields(),
            source.wkbType(),
            source.sourceCrs(),
        )

        # Get update policy
        feedback.pushInfo(tr("Get update policy..."))
        update_policy_index = self.parameterAsEnum(parameters, self.UPDATE_POLICY, context)
        update_policy = self.UPDATE_POLICY_VALUES[update_policy_index]
        feedback.pushInfo(tr(f"* Update policy : {update_policy}"))
        feedback.pushInfo("")

        # Get preserve references parameter
        feedback.pushInfo(tr("Get parameter preserve positions..."))
        p_index = self.parameterAsEnum(parameters, self.PRESERVE_START_END_POSITIONS, context)
        preserve_start_end_positions = self.PRESERVE_START_END_POSITIONS_VALUES[p_index]
        feedback.pushInfo(tr(f"* Preserve start & end positions : {preserve_start_end_positions}"))
        feedback.pushInfo("")

        # Check for cancellation
        cancel_message = tr("The processing has been cancelled by the user.")
        if feedback.isCanceled():
            raise QgsProcessingException(cancel_message)

        # Get PostgreSQL layer data provider and connection
        feedback.pushInfo(tr("Get PostgreSQL connection..."))
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        if not connection:
            error_message = tr(
                "* Could not create a connection to the database with the given connection name."
            )
            raise QgsProcessingException(error_message)
        feedback.pushInfo(tr(f"* Using connection : {connection_name}"))
        feedback.pushInfo("")
        pg_conn = connect(QgsDataSourceUri(connection.uri()).connectionInfo())

        # Check for cancellation
        if feedback.isCanceled():
            pg_conn.close()
            raise QgsProcessingException(cancel_message)

        # For each feature, request the database and get the updated geometry or references
        feedback.pushInfo(tr("Processing features..."))
        feature_count = source.featureCount()
        unchanged_count = 0
        for idx, feature in enumerate(source.getFeatures()):
            # Check for cancellation
            if feedback.isCanceled():
                pg_conn.close()
                raise QgsProcessingException(cancel_message)

            # Create a new feature for the output layer
            output_feature = QgsFeature()

            # Keep attributes
            attributes = feature.attributes()
            output_feature.setFields(feature.fields())
            output_feature.setAttributes(attributes)

            # Set geometry from the source feature
            output_feature.setGeometry(feature.geometry())

            # feedback.pushInfo(tr(f"Feature {idx}"))

            # Calculate references from the geometry
            # We do it if update_policy is "update_references"
            # and also for update_geometry
            # if the parameter UPDATE_REFERENCES is set to yes
            if update_policy == "update_references" or (
                update_policy == "update_geometry" and preserve_start_end_positions == "yes"
            ):
                # We need to update the feature references from the feature geometry
                if QgsWkbTypes.geometryType(source.wkbType()) == QgsWkbTypes.PointGeometry:
                    references_keys = ["road_code", "marker_code", "abscissa", "side", "offset", "cumulative"]

                    # Do not process empty geometry
                    if not feature.geometry():
                        unchanged_count += 1
                        continue

                    # Get the references from the database for the point
                    point = feature.geometry().asPoint()
                    point_references = self.getReferencesFromLonLat(connection, point.x(), point.y(), pg_conn)
                    has_changed = False
                    if point_references:
                        # print(f"references for {feature.id()} : {point_references}")
                        for ref in references_keys:
                            if ref not in feature.fields().names():
                                continue
                            output_feature[ref] = feature[ref]
                            if feature[ref] != point_references.get(ref):
                                output_feature[ref] = point_references.get(ref)
                                has_changed = True
                    if not has_changed:
                        unchanged_count += 1

                # Linestring layer
                else:
                    # Do not process empty geometry
                    if not feature.geometry():
                        unchanged_count += 1
                        continue

                    # Get start and end point geometries
                    # depending on whether the geometry is simple or multipart
                    is_simple = QgsWkbTypes.isSingleType(source.wkbType())
                    if is_simple:
                        first_vertex = feature.geometry().asPolyline()[0]
                        last_vertex = feature.geometry().asPolyline()[-1]
                    else:
                        first_vertex = feature.geometry().asMultiPolyline()[0][0]
                        last_vertex = feature.geometry().asMultiPolyline()[-1][-1]

                    # Result dictionary containing the updated references for the line start and end points
                    line_references = {}

                    # Get the references from the database for the line start point
                    line_references["start"] = self.getReferencesFromLonLat(
                        connection, first_vertex.x(), first_vertex.y(), pg_conn
                    )
                    # print(f"start references for {feature.id()} : {line_references["start"]}")

                    # Get the references from the database for the line end point
                    line_references["end"] = self.getReferencesFromLonLat(
                        connection, last_vertex.x(), last_vertex.y(), pg_conn
                    )
                    # print(f"end references for {feature.id()} : {line_references["end"]}")

                    # If the geometry orientation is wrong
                    # (end point is at a lower reference than the start point)
                    # we invert the start and end references
                    if line_references["start"] and line_references["end"]:
                        start_value = line_references["start"]["marker_code"] * 1000000
                        start_value += line_references["start"]["abscissa"]
                        end_value = line_references["end"]["marker_code"] * 1000000
                        end_value += line_references["end"]["abscissa"]
                        if start_value > end_value:
                            start_references = line_references["start"]
                            end_references = line_references["end"]
                            line_references["start"] = end_references
                            line_references["end"] = start_references

                    # Update feature attributes
                    has_changed = False
                    # road_code, offset & side of the start point
                    for attribute in ("road_code", "offset", "side"):
                        # Do not change the references for the ouput feature
                        if attribute not in feature.fields().names():
                            continue
                        output_feature[attribute] = feature[attribute]
                        if not line_references["start"]:
                            continue
                        # print(f'{feature.id()} - {attribute} = {line_references["start"][attribute]}')
                        if feature[attribute] != line_references["start"][attribute]:
                            output_feature[attribute] = line_references["start"][attribute]
                            has_changed = True

                    # start_ and end_ attributes
                    references_keys = ["marker_code", "abscissa", "cumulative"]
                    for place in ("start", "end"):
                        # Do not change the references for the ouput feature
                        for ref in references_keys:
                            if f"{place}_{ref}" not in feature.fields().names():
                                continue
                            output_feature[f"{place}_{ref}"] = feature[f"{place}_{ref}"]
                            if not line_references[place]:
                                continue
                            # print(f'{feature.id()} - {place}_{ref} = {line_references[place][ref]}')
                            if feature[f"{place}_{ref}"] != line_references[place][ref]:
                                output_feature[f"{place}_{ref}"] = line_references[place][ref]
                                has_changed = True
                    if not has_changed:
                        unchanged_count += 1

            # We need to update the feature geometry from the feature references
            if update_policy == "update_geom":
                # We get the references from the feature
                # We use the output_feature since it could have been updated above
                # if the update_policy was update_geom and preserve_start_end_positions == 'yes'
                if QgsWkbTypes.geometryType(source.wkbType()) == QgsWkbTypes.PointGeometry:
                    references_keys = ["road_code", "marker_code", "abscissa", "side", "offset"]
                    references = {
                        a: output_feature.attribute(a)
                        for a in references_keys
                        if a in output_feature.fields().names()
                    }
                else:
                    references_keys = [
                        "road_code",
                        "start_marker_code",
                        "start_abscissa",
                        "end_marker_code",
                        "end_abscissa",
                        "side",
                        "offset",
                    ]
                    references = {
                        a: output_feature.attribute(a)
                        for a in references_keys
                        if a in output_feature.fields().names()
                    }

                # Set default values for side and offset if they are not present in the feature attributes
                if "side" not in references:
                    references["side"] = "right"
                if "offset" not in references:
                    references["offset"] = 0

                # Get WKT geometry from the database
                geometry_type = "point"
                if QgsWkbTypes.geometryType(source.wkbType()) == QgsWkbTypes.LineGeometry:
                    geometry_type = "linestring"

                # Request the database for the updated geometry
                wkt_geometry = self.getWktFromReferences(connection, geometry_type, references, pg_conn)

                # Set the updated geometry to the feature
                if wkt_geometry:
                    qgis_geometry = QgsGeometry.fromWkt(wkt_geometry)
                    output_feature.setGeometry(qgis_geometry)
                else:
                    unchanged_count += 1

            # Add the feature to the output layer sink
            sink.addFeature(output_feature, QgsFeatureSink.FastInsert)

            # Set progress
            feedback.setProgress(int(100 * idx / feature_count))

        # Close connection to the database
        pg_conn.close()

        # End messages
        feedback.pushInfo(tr(f"* {feature_count} features processed."))
        feedback.pushInfo(tr(f"* {unchanged_count} features left unchanged."))

        return {self.OUTPUT: dest_id}
