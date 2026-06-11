import json
import typing

from qgis.core import (
    QgsAbstractDatabaseProviderConnection,
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

    def name(self):
        return "update_managed_objects"

    def displayName(self):
        return tr("Update managed objects from the road graph")

    def group(self):
        return tr("Editing")

    def groupId(self):
        return f"{plugin_name_normalized()}_editing"

    def shortHelpString(self):
        short_help = tr(
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
        return short_help

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

        # Output layer
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT, tr("Output layer"), QgsProcessing.TypeVectorAnyGeometry
            )
        )

    def supportInPlaceEdit(self, layer: QgsVectorLayer) -> bool:
        """Determine if the algorithm can be run in place on the given layer."""
        # must be spatial
        if not layer.isSpatial():
            return False

        return True

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
        road_code: str = "",
    ) -> dict:
        """
        Get the reference of a QGIS feature from the database
        """
        # print(tr(f"Requesting database for reference of point with coordinates : {longitude}, {latitude}"))
        sql_road_code = "NULL"
        if road_code:
            sql_road_code = f"'{road_code}'"
        sql = f"""
        SELECT
            road_graph.get_reference_from_point(
                ST_PointFromText('POINT({longitude} {latitude})', 2154),
                {sql_road_code},
                False
            )::json AS ref
        ;
        """
        # print(sql)
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))
        references = ""
        for a in data:
            references = a[0] if a else "{}"
        if references:
            return json.loads(references)

        return {}

    def getWktFromReferences(
        self, connection: QgsAbstractDatabaseProviderConnection, geometry_type: str, references: dict
    ) -> QgsGeometry:
        """
        Get the geometry of a feature from the database from its reference
        """
        # print(tr(f"Requesting database for WKT of feature with reference : {references}"))
        if geometry_type == "point":
            sql = f"""
            WITH get_geom AS (
                SELECT
                road_graph.get_road_point_from_reference(
                    '{references["road_code"]}',
                    {references["marker_code"]},
                    {references["abscissa"]},
                    {references["offset"]},
                    '{references["side"]}'
                )->'geom' AS geom
            )
            SELECT
            CASE
                WHEN geom = 'null'::jsonb THEN NULL
                ELSE ST_AsText(
                    ST_GeomFromGeoJSON(
                        geom
                    )
                )
            END
            FROM get_geom
            """
        else:
            sql = f"""
            WITH get_geom AS (
                SELECT
                road_graph.get_road_substring_from_references(
                    '{references["road_code"]}',
                    {references["start_marker_code"]},
                    {references["start_abscissa"]},
                    {references["end_marker_code"]},
                    {references["end_abscissa"]},
                    {references["offset"]},
                    '{references["side"]}'
                )->'geom' AS geom
            )
            SELECT
            CASE
                WHEN geom = 'null'::jsonb THEN NULL
                ELSE ST_AsText(
                    ST_GeomFromGeoJSON(
                        geom
                    )
                )
            END
            FROM get_geom
            """
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
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

        # Check for cancellation
        if feedback.isCanceled():
            raise QgsProcessingException(cancel_message)

        # For each feature, request the database and get the updated geometry or references
        feedback.pushInfo(tr("Processing features..."))
        feature_count = source.featureCount()
        unchanged_count = 0
        for idx, feature in enumerate(source.getFeatures()):
            # Check for cancellation
            if feedback.isCanceled():
                raise QgsProcessingException(cancel_message)

            # Create a new feature for the output layer
            output_feature = QgsFeature()

            # Keep attributes
            attributes = feature.attributes()
            output_feature.setFields(feature.fields())
            output_feature.setAttributes(attributes)

            # Set geometry from the source feature
            output_feature.setGeometry(feature.geometry())

            feedback.pushInfo(tr(f"Feature {idx}"))

            # Request the database for the updated references
            # Set the attributes or the geometry based on the update policy
            if update_policy == "update_geom":
                # We need to update the feature geometry from the feature references
                # Request the database for the updated geometry
                if QgsWkbTypes.geometryType(source.wkbType()) == QgsWkbTypes.PointGeometry:
                    references_keys = ["road_code", "marker_code", "abscissa", "side", "offset"]
                    references = {a: feature[a] for a in references_keys if a in feature.fields().names()}
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
                    references = {a: feature[a] for a in references_keys if a in feature.fields().names()}

                # Set default values for side and offset if they are not present in the feature attributes
                if "side" not in references:
                    references["side"] = "right"
                if "offset" not in references:
                    references["offset"] = 0

                # Get WKT geometry from the database
                geometry_type = "point"

                if QgsWkbTypes.geometryType(source.wkbType()) == QgsWkbTypes.LineGeometry:
                    geometry_type = "linestring"
                wkt_geometry = self.getWktFromReferences(connection, geometry_type, references)

                # Set the updated geometry to the feature
                if wkt_geometry:
                    qgis_geometry = QgsGeometry.fromWkt(wkt_geometry)
                    output_feature.setGeometry(qgis_geometry)
                else:
                    unchanged_count += 1

            else:
                # We need to update the feature references from the feature geometry
                if QgsWkbTypes.geometryType(source.wkbType()) == QgsWkbTypes.PointGeometry:
                    references_keys = ["road_code", "marker_code", "abscissa", "side", "offset", "cumulative"]
                    # Get the updated geometry from the database for the point
                    point = feature.geometry().asPoint()
                    references = self.getReferencesFromLonLat(connection, point.x(), point.y())
                    has_changed = False
                    if references:
                        # print(f"references for {feature.id()} : {references}")
                        for ref in references_keys:
                            if ref not in feature.fields().names():
                                continue
                            output_feature[ref] = feature[ref]
                            if feature[ref] != references.get(ref):
                                output_feature[ref] = references.get(ref)
                                has_changed = True
                    if not has_changed:
                        unchanged_count += 1

                # Linestring layer
                else:
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
                    result = {}

                    # Get the references from the database for the line start point
                    result["start"] = self.getReferencesFromLonLat(
                        connection, first_vertex.x(), first_vertex.y()
                    )
                    # print(f"start references for {feature.id()} : {result["start"]}")

                    # Get the references from the database for the line end point
                    result["end"] = self.getReferencesFromLonLat(connection, last_vertex.x(), last_vertex.y())
                    # print(f"end references for {feature.id()} : {result["end"]}")

                    # Update feature attributes
                    has_changed = False
                    # road_code, offset & side of the start point
                    for attribute in ("road_code", "offset", "side"):
                        if attribute not in feature.fields().names():
                            continue
                        output_feature[attribute] = feature[attribute]
                        if not result["start"]:
                            continue
                        # print(f'{feature.id()} - {attribute} = {result["start"][attribute]}')
                        if feature[attribute] != result["start"][attribute]:
                            output_feature[attribute] = result["start"][attribute]
                            has_changed = True

                    # start_ and end_ attributes
                    references_keys = ["marker_code", "abscissa", "cumulative"]
                    for place in ("start", "end"):
                        for ref in references_keys:
                            if f"{place}_{ref}" not in feature.fields().names():
                                continue
                            output_feature[f"{place}_{ref}"] = feature[f"{place}_{ref}"]
                            if not result[place]:
                                continue
                            # print(f'{feature.id()} - {place}_{ref} = {result[place][ref]}')
                            if feature[f"{place}_{ref}"] != result[place][ref]:
                                output_feature[f"{place}_{ref}"] = result[place][ref]
                                has_changed = True
                    if not has_changed:
                        unchanged_count += 1

            # Add the feature to the output layer sink
            sink.addFeature(output_feature, QgsFeatureSink.FastInsert)

            # Set progress
            feedback.setProgress(int(100 * idx / feature_count))

        feedback.pushInfo(tr(f"* {feature_count} features processed."))
        feedback.pushInfo(tr(f"* {unchanged_count} features left unchanged."))

        return {self.OUTPUT: dest_id}
