import json
import time

from typing import Tuple

from qgis import processing
from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsProcessing,
    QgsProcessingException,
    QgsProcessingFeedback,
    QgsProcessingOutputNumber,
    QgsProcessingOutputString,
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


class ImportData(BaseProcessingAlgorithm):
    # Parameters
    CONNECTION_NAME = "CONNECTION_NAME"
    INPUT_EDGES = "INPUT_EDGES"
    INPUT_MARKERS = "INPUT_MARKERS"
    OUTPUT_STATUS = "OUTPUT_STATUS"
    OUTPUT_STRING = "OUTPUT_STRING"

    # Required fields
    REQUIRED_FIELDS_EDGES = (
        "edge_order",
        "road_code",
        "road_class",
        "road_type",
        "road_class_code",
        "is_active",
        "road_topic",
        "is_access_road",
    )
    REQUIRED_FIELDS_MARKERS = (
        "road_code",
        "code",
        "abscissa",
        "is_virtual",
    )

    # CRS
    TARGET_CRS = QgsCoordinateReferenceSystem("EPSG:2154")

    def name(self):
        return "import_data"

    def displayName(self):
        return tr("Import data from template")

    def group(self):
        return tr("Editing")

    def groupId(self):
        return f"{plugin_name_normalized()}_editing"

    def shortHelpString(self):
        short_help = tr(
            "This algorithm will allow to import data into the database "
            "\n"
            "\n"
            "You will need to provide data for the edges and the markers "
            "in the correct format. "
            "You can use the template provided with the plugin as an example."
        )
        return short_help

    def initAlgorithm(self, config):
        project = QgsProject.instance()
        connection_name = get_connection_name(project)
        param = QgsProcessingParameterProviderConnection(
            self.CONNECTION_NAME,
            tr("PostgreSQL connection to the database"),
            "postgres",
            defaultValue=connection_name,
            optional=False,
        )
        param.setHelp(tr("The database where the plugin structure will be installed."))
        self.addParameter(param)

        # Edges source table
        param = QgsProcessingParameterFeatureSource(
            self.INPUT_EDGES,
            tr("Edges (with road codes, types, classes, edge order, etc.)"),
            [QgsProcessing.SourceType.TypeVectorLine],
        )
        param.setHelp(
            tr("Edges source data. You can use the template provided with the plugin as an example")
        )
        self.addParameter(param)

        # Markers source table
        param = QgsProcessingParameterFeatureSource(
            self.INPUT_MARKERS,
            tr("Markers (with road, code, abscissa, etc.)"),
            [QgsProcessing.SourceType.TypeVectorPoint],
        )
        param.setHelp(
            tr("Markers source data. You can use the template provided with the plugin as an example")
        )
        self.addParameter(param)

        # OUTPUTS
        # Add output for status (integer)
        self.addOutput(QgsProcessingOutputNumber(self.OUTPUT_STATUS, tr("Output status")))
        # Add output for message
        self.addOutput(QgsProcessingOutputString(self.OUTPUT_STRING, tr("Output message")))

    def checkParameterValues(self, parameters, context):
        """Check the validity of the parameters before running the algorithm."""
        # Connection name
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        if not connection_name:
            return False, tr("No valid database connection provided.")
        metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
        connection = metadata.findConnection(connection_name)
        if not connection:
            return False, tr("Could not create a connection to the database with the given connection name.")

        # Required fields for edges and markers
        # Edges
        source = self.parameterAsSource(parameters, self.INPUT_EDGES, context)
        fields = source.fields().names()
        hasRequiredFields, message = self.hasRequiredFields(fields, self.REQUIRED_FIELDS_EDGES)
        if not hasRequiredFields:
            return False, message

        # Markers
        source = self.parameterAsSource(parameters, self.INPUT_MARKERS, context)
        fields = source.fields().names()
        hasRequiredFields, message = self.hasRequiredFields(fields, self.REQUIRED_FIELDS_MARKERS)
        if not hasRequiredFields:
            return False, message

        return True, ""

    def hasRequiredFields(self, fields: list, needed_fields: list) -> Tuple[bool, str]:
        """Check if the layer has the required fields to run the algorithm."""
        # Check if all needed fields are present in the layer
        missing_fields = []
        for field in needed_fields:
            if field not in fields:
                missing_fields.append(field)
        if missing_fields:
            print(f"Missing fields : {missing_fields}")
            message = tr("The input layer is missing the following required fields: ")
            message += ", ".join(missing_fields)
            return False, message

        return True, tr("All required fields are present")

    def cleanUp(
        self, connection_name: str, temp_schema: str, temp_tables: list[str], feedback: QgsProcessingFeedback
    ) -> None:
        """Clean up the temporary tables after the algorithm has been run."""
        for temp_table in temp_tables:
            sql = f'DROP TABLE IF EXISTS "{temp_schema}"."{temp_table}"'
            connection = (
                QgsProviderRegistry.instance().providerMetadata("postgres").findConnection(connection_name)
            )
            try:
                connection.executeSql(sql)
                # feedback.pushInfo(tr(f"* Temporary table {temp_table} has been dropped"))
            except QgsProviderConnectionException as e:
                msg = tr("* Failed to drop temporary table")
                msg += f" {temp_table} ({e!s})"
                feedback.pushInfo(msg)

    def processAlgorithm(self, parameters, context, feedback):
        """Run the algorithm to import data into the database."""
        msg = ""
        status = 1

        # Parameters
        connection_name = self.parameterAsConnectionName(parameters, self.CONNECTION_NAME, context)
        random_time = str(time.time()).replace(".", "")

        # Import edges
        feedback.pushInfo("")
        feedback.pushInfo(tr("IMPORT EDGES SOURCE LAYER INTO TEMPORARY TABLE"))
        temp_schema = "public"
        edges_temp_table = "temp_edges_" + random_time
        processing.run(
            "gdal:importvectorintopostgisdatabaseavailableconnections",
            {
                "DATABASE": connection_name,
                "INPUT": parameters[self.INPUT_EDGES],
                "SHAPE_ENCODING": "",
                "GTYPE": 4,
                "A_SRS": None,
                "T_SRS": self.TARGET_CRS,
                "S_SRS": None,
                "SCHEMA": temp_schema,
                "TABLE": edges_temp_table,
                "PK": "zid",
                "PRIMARY_KEY": "",
                "GEOCOLUMN": "geom",
                "DIM": 0,
                "SIMPLIFY": "",
                "SEGMENTIZE": "",
                "SPAT": None,
                "CLIP": False,
                "WHERE": "",
                "GT": "",
                "OVERWRITE": True,
                "APPEND": False,
                "ADDFIELDS": False,
                "LAUNDER": False,
                "INDEX": False,
                "SKIPFAILURES": False,
                "MAKEVALID": False,
                "PROMOTETOMULTI": False,
                "PRECISION": False,
                "OPTIONS": "",
            },
            context=context,
            feedback=feedback,
        )
        feedback.pushInfo(
            tr("* Edges source layer has been imported into temporary table") + " " + edges_temp_table
        )

        # Import markers
        feedback.pushInfo("")
        feedback.pushInfo(tr("IMPORT MARKERS SOURCE LAYER INTO TEMPORARY TABLE"))
        temp_schema = "public"
        markers_temp_table = "temp_markers_" + random_time
        processing.run(
            "gdal:importvectorintopostgisdatabaseavailableconnections",
            {
                "DATABASE": connection_name,
                "INPUT": parameters[self.INPUT_MARKERS],
                "SHAPE_ENCODING": "",
                "GTYPE": 3,
                "A_SRS": None,
                "T_SRS": self.TARGET_CRS,
                "S_SRS": None,
                "SCHEMA": temp_schema,
                "TABLE": markers_temp_table,
                "PK": "zid",
                "PRIMARY_KEY": "",
                "GEOCOLUMN": "geom",
                "DIM": 0,
                "SIMPLIFY": "",
                "SEGMENTIZE": "",
                "SPAT": None,
                "CLIP": False,
                "WHERE": "",
                "GT": "",
                "OVERWRITE": True,
                "APPEND": False,
                "ADDFIELDS": False,
                "LAUNDER": False,
                "INDEX": False,
                "SKIPFAILURES": False,
                "MAKEVALID": False,
                "PROMOTETOMULTI": False,
                "PRECISION": False,
                "OPTIONS": "",
            },
            context=context,
            feedback=feedback,
        )
        feedback.pushInfo(
            tr("* Markers source layer has been imported into temporary table") + " " + markers_temp_table
        )

        # Convert edges and markers data from temporary tables to the production schema
        feedback.pushInfo("")
        feedback.pushInfo(
            tr("CONVERT THE EDGES AND MARKERS DATA FROM TEMPORARY TABLES TO THE PRODUCTION SCHEMA")
        )
        sql = f"""
            SELECT road_graph.import_data_from_template_tables(
                '{temp_schema}',
                '{edges_temp_table}',
                '{markers_temp_table}'
            ) AS result
        """
        connection = (
            QgsProviderRegistry.instance().providerMetadata("postgres").findConnection(connection_name)
        )
        try:
            data = connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            self.cleanUp(connection_name, temp_schema, [edges_temp_table, markers_temp_table], feedback)
            raise QgsProcessingException(str(e))
        result = None
        for a in data:
            result = a[0] if a else None
        empty_result = {
            "status": "error",
            "message": "No result returned from the database import function.",
            "data": None,
            "edges_count": 0,
            "nodes_count": 0,
            "roads_count": 0,
            "markers_count": 0,
        }
        json_result = json.loads(result) if result else empty_result

        # Check errors
        if json_result.get("status") != "success":
            error_message = tr("An error occurred while importing the data into the database: ")
            error_message += json_result.get("message", "Unknown error")
            if json_result.get("data"):
                error_message += "\n\n" + tr("* Number: ") + str(json_result["data"]["number"])
                error_message += "\n" + tr("* Details: ") + json_result["data"]["details"]

            status = 0
            self.cleanUp(connection_name, temp_schema, [edges_temp_table, markers_temp_table], feedback)
            raise QgsProcessingException(error_message)

        # End message
        msg = tr("The edges and markers data have been imported successfully into the database.")
        msg += tr("\n\nSummary of the import:") + "\n"
        msg += tr("- Number of edges imported: ") + str(json_result.get("edges_count", 0)) + "\n"
        msg += tr("- Number of nodes imported: ") + str(json_result.get("nodes_count", 0)) + "\n"
        msg += tr("- Number of roads imported: ") + str(json_result.get("roads_count", 0)) + "\n"
        msg += tr("- Number of markers imported: ") + str(json_result.get("markers_count", 0)) + "\n"
        feedback.pushInfo("")
        feedback.pushInfo(msg)
        status = 1

        # clean up temporary tables
        self.cleanUp(connection_name, temp_schema, [edges_temp_table, markers_temp_table], feedback)

        return {self.OUTPUT_STATUS: status, self.OUTPUT_STRING: msg}
