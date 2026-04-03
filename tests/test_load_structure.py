"""Tests for Processing algorithms."""

import unittest

from pathlib import Path

import psycopg

from qgis import processing
from qgis.core import (
    QgsProcessingException,
    QgsProviderConnectionException,
    QgsProviderRegistry,
)

from roadnetwork.plugin_tools.feedback import LoggerProcessingFeedBack
from roadnetwork.plugin_tools.resources import (
    schema_version,
)
from roadnetwork.processing.database import CreateDatabaseStructure, UpgradeDatabaseStructure
from roadnetwork.processing.provider import Provider

# This list must not be changed
# as it correspond to the list of tables
# created for the first version
TABLES_FOR_FIRST_VERSION = [
    "edges",
    "editing_sessions",
    "glossary_road_class",
    "markers",
    "metadata",
    "nodes",
    "roads",
    "v_road_without_zero_marker",
]

# Expected list of tables for current version
# Must be changed any time the SQL structure is changed
TABLES_FOR_CURRENT_VERSION = [
    "edges",
    "editing_sessions",
    "glossary_road_class",
    "markers",
    "metadata",
    "nodes",
    "roads",
    "v_road_without_zero_marker",
]


def test_processing_create(processing_provider: Provider):
    """Test the processing algorithm for creating the database structure."""

    params = {
        "CONNECTION_NAME": "test",
        "OVERRIDE": True,
    }

    feedback = LoggerProcessingFeedBack()

    # Run create database structure alg
    alg = f"{processing_provider.id()}:create_database_structure"
    processing_output = processing.run(alg, params, feedback=feedback)

    assert processing_output["OUTPUT_STATUS"] == 1
    assert processing_output["OUTPUT_VERSION"] == schema_version()


def test_upgrade_from(
    db_schema: str,
    db_install_version: int,
    db_connection: psycopg.Connection,
    processing_provider: Provider,
    data: Path,
):
    """Test the algorithms for creating and updating the database structure."""

    current_version = schema_version()

    assert db_install_version is not None, "This test require at least one available upgrade"
    assert current_version >= db_install_version, (
        "Current schema version cannot be lower than install version"
    )

    # Get the installation dir of the previous version
    test_version = 1
    install_dir = data.joinpath(f"install-version-{test_version}", "sql")
    assert install_dir.exists()

    feedback = LoggerProcessingFeedBack()

    # Create the database from the latest update
    CreateDatabaseStructure.create_database(
        "test",
        db_schema,
        version=db_install_version,
        override=True,
        install_dir=install_dir,
        feedback=feedback,
    )

    case = unittest.TestCase()

    cursor = db_connection.cursor()

    # Check the list of tables and views from the database
    cursor.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{db_schema}'
        ORDER BY table_name
        """
    )
    records = cursor.fetchall()
    result = [r[0] for r in records]

    # Expected tables in the specific version written above at the beginning of the test.
    # DO NOT CHANGE HERE, change below at the end of the test.
    case.assertCountEqual(TABLES_FOR_FIRST_VERSION, result)

    assert result == TABLES_FOR_FIRST_VERSION

    # Check if the version has been written in the metadata table
    sql = f"""
        SELECT me_version
        FROM {db_schema}.metadata
        WHERE me_status = 1
        ORDER BY me_version_date DESC
        LIMIT 1;
    """
    cursor.execute(sql)
    record = cursor.fetchone()
    assert record is not None
    assert int(record[0]) == db_install_version

    # Run the update database structure alg
    # Since the structure has been created with db_install_version above
    # The expected list of tables
    feedback.pushDebugInfo("Update the database")

    UpgradeDatabaseStructure.upgrade_database(
        "test",
        db_schema,
        run_migrations=True,
        feedback=feedback,
    )
    # Check if the version has been written in the metadata table
    sql = f"""
        SELECT me_version
        FROM {db_schema}.metadata
        WHERE me_status = 1
        ORDER BY me_version_date DESC
        LIMIT 1;
    """
    cursor.execute(sql)
    record = cursor.fetchone()
    assert record is not None
    assert int(record[0]) == current_version

    # Check the list of tables
    cursor.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{db_schema}'
        ORDER BY table_name
        """
    )
    records = cursor.fetchall()
    result = [r[0] for r in records]
    case.assertCountEqual(TABLES_FOR_CURRENT_VERSION, result)

    # Close connection
    db_connection.close()


def test_reset_test_data(processing_provider: Provider, data: Path):
    """Reset the test data: vaccum the database and import again the test data"""

    params = {
        "CONNECTION_NAME": "test",
        "OVERRIDE": True,
    }

    feedback = LoggerProcessingFeedBack()

    # Run create database structure alg
    alg = f"{processing_provider.id()}:create_database_structure"
    processing_output = processing.run(alg, params, feedback=feedback)

    assert processing_output["OUTPUT_STATUS"] == 1
    assert processing_output["OUTPUT_VERSION"] == schema_version()

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Remove previous data
    sql = """
    -- schema road_graph
    TRUNCATE road_graph.roads RESTART IDENTITY CASCADE;
    TRUNCATE road_graph.nodes RESTART IDENTITY CASCADE;
    TRUNCATE road_graph.edges RESTART IDENTITY CASCADE;
    TRUNCATE road_graph.markers RESTART IDENTITY CASCADE;
    TRUNCATE road_graph.editing_sessions RESTART IDENTITY CASCADE;
    TRUNCATE road_graph.managed_objects RESTART IDENTITY CASCADE;
    TRUNCATE road_graph.glossary_road_class RESTART IDENTITY CASCADE;
    -- schema editing_session
    TRUNCATE editing_session.roads RESTART IDENTITY CASCADE;
    TRUNCATE editing_session.nodes RESTART IDENTITY CASCADE;
    TRUNCATE editing_session.edges RESTART IDENTITY CASCADE;
    TRUNCATE editing_session.markers RESTART IDENTITY CASCADE;
    TRUNCATE editing_session.editing_sessions RESTART IDENTITY CASCADE;
    TRUNCATE editing_session.managed_objects RESTART IDENTITY CASCADE;
    TRUNCATE editing_session.glossary_road_class RESTART IDENTITY CASCADE;
    """
    try:
        connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    # Import tests data
    sql_file = data.joinpath("sql", "test_data.sql")
    with open(sql_file, "r") as f:
        sql = f.read()
        try:
            connection.executeSql(sql)
        except QgsProviderConnectionException as e:
            raise QgsProcessingException(str(e))


def test_create_editing_session(processing_provider: Provider):
    """Test the creation of an editing session and the cloning of data into the editing_session schema."""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Create an editing session
    sql = """
    INSERT INTO road_graph.editing_sessions (
        "id", "label", "author",
        "created_at", "updated_at", "description",
        "status", "unique_code",
        "geom"
    ) VALUES (
        1, 'test editing session', '3liz',
        '2026-03-26T17:38:33.000', '2026-03-26T17:38:33.000', 'For test purpose only',
        'created', 'editing_session_2026_03_26_05_38_33',
        ST_SetSRID(
            ST_GeomFromText(
                'Polygon ((
                    474185.40000000002328306 6896501.90000000037252903,
                    475025 6896180.40000000037252903,
                    475418 6894554.79999999981373549,
                    473595.90000000002328306 6894268.90000000037252903,
                    472577.59999999997671694 6894554.79999999981373549,
                    472684.79999999998835847 6896698.5,
                    474185.40000000002328306 6896501.90000000037252903
                ))'
            ),
            2154
        )
    )
    ;
    """
    try:
        connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    # Clone data into the schema editing_session for the created editing session
    params = {
        "CONNECTION_NAME": "test",
    }
    alg = f"{processing_provider.id()}:create_editing_session"
    feedback = LoggerProcessingFeedBack()
    processing_output = processing.run(alg, params, feedback=feedback)
    assert processing_output["OUTPUT_STATUS"] == 1

    # Check the number of objects in the editing_session schema
    sql = """
        SELECT
            jsonb_array_length(cloned_ids['edges']),
            jsonb_array_length(cloned_ids['nodes']),
            jsonb_array_length(cloned_ids['markers']),
            jsonb_array_length(cloned_ids['roads'])
        FROM road_graph.editing_sessions
        WHERE id = 1
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    stats = None
    for a in data:
        stats = a if a else None
    assert stats is not None
    # Check the number of edges, nodes, markers and roads
    assert (stats[0], stats[1], stats[2], stats[3]) == (173, 213, 165, 5)


def test_create_edge(processing_provider: Provider):
    """Test the creation of a new edge in the editing session and the calculation of its attributes"""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Create a new road and a new edges
    sql = """
    INSERT INTO editing_session.roads (road_code, road_type, road_class) VALUES (
        'T001', 'road', 'Départementale'
    )
    ;
    INSERT INTO editing_session.edges (geom, road_code) VALUES (
        ST_SetSRID(
            ST_GeomFromText(
                'LineString (
                    475154 6895454, 475114 6895374, 475085 6895256,
                    475031 6895181, 474873 6895084, 474792 6894903, 474709 6894739
                )'
            ),
            2154
        ),
        'T001'
    )
    ;
    SELECT
        e.id, e.road_code,
        e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative
    FROM editing_session.edges AS e
    WHERE e.road_code = 'T001'
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edge = None
    for a in data:
        edge = a if a else None
    assert edge is not None
    # Check the edge data
    assert [edge[0], edge[1]] == [7830, "T001"]

    # Check 2 new nodes have been created
    assert [edge[2], edge[3]] == [5758, 5759]

    # Check the references have been calculated for this new edge
    assert [edge[4], edge[5], edge[6]] == [0, 0.0, 0.0]
    assert [edge[7], edge[8], edge[9]] == [0, 870.88, 870.88]


def test_cut_edge_by_node(processing_provider: Provider):
    """Add a node in the middle of the new edge
    which should cut the new edge in two parts
    and check the attributes of the two new edges"""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)
    sql = """
        INSERT INTO editing_session.nodes (geom) VALUES (
            ST_SetSRID(
                ST_GeomFromText(
                    'POINT (474818.54 6894961.66)'
                ),
                2154
            )
        );
        SELECT
            e.id, e.road_code,
            e.start_node, e.end_node,
            e.start_marker, e.start_abscissa, e.start_cumulative,
            e.end_marker, e.end_abscissa, e.end_cumulative,
            e.previous_edge_id, e.next_edge_id
        FROM editing_session.edges AS e
        WHERE e.road_code = 'T001'
            ORDER BY e.id
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)

    # Check the number of edges for the road
    assert len(edges) == 2
    assert edges is not None

    # edge 1
    assert edges[0] is not None
    # id and road code
    assert [edges[0][0], edges[0][1]] == [7830, "T001"]
    # Check the start and end nodes
    assert [edges[0][2], edges[0][3]] == [5758, 5760]
    # Check the references have been calculated for this edge
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 0.0, 0.0]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [0, 622.67, 622.67]
    # Check the previous and next edges ids
    assert [edges[0][10], edges[0][11]] == [None, 7831]

    # edge 2
    assert edges[1] is not None
    # id and road code
    assert [edges[1][0], edges[1][1]] == [7831, "T001"]
    # Check the start and end nodes
    assert [edges[1][2], edges[1][3]] == [5760, 5759]
    # Check the references have been calculated for this edge
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 622.67, 622.67]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [0, 870.88, 870.88]
    # Check the previous and next edges ids
    assert [edges[1][10], edges[1][11]] == [7830, None]


def test_insert_edge_at_the_end_of_a_road(processing_provider: Provider):
    """Insert a new edge at the end of the road T001 and
    check the attributes of the new edge and the previous last edge
    """
    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Insert a new edge at the end (touching) of the last edge
    # specifying the previous edge id
    sql = """
    INSERT INTO editing_session.edges (
        geom, road_code
    ) VALUES (
        ST_SetSRID(
            ST_GeomFromText(
                'LineString (
                    474709 6894739, 474677.14 6894702.57
                )'
            ),
            2154
        ),
        'T001'
    )
    ;
    SELECT
        e.id, e.road_code,
        e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative,
        e.previous_edge_id, e.next_edge_id
    FROM editing_session.edges AS e
    WHERE e.road_code = 'T001'
        ORDER BY e.id
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)
    # Check the number of edges for the road
    assert len(edges) == 3

    # Check the previous and next edges ids have been calculated for the new edge
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert [edges[0][10], edges[0][11]] == [None, 7831]
    assert [edges[1][10], edges[1][11]] == [7830, 7832]
    assert [edges[2][10], edges[2][11]] == [7831, None]

    # Check the references have been calculated for all the edges of the road
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 0.0, 0.0]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [0, 622.67, 622.67]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 622.67, 622.67]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [0, 870.88, 870.88]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [0, 870.88, 870.88]
    assert [edges[2][7], edges[2][8], edges[2][9]] == [0, 919.28, 919.28]


def test_change_edge_road(processing_provider: Provider):
    """Create a new road and change the middle edge of a road
    to be part of this new road.
    Check the references have been calculated for the modified edge
    and for the remaining edges of the old road
    """

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Insert a new edge at the end (touching) of the last edge
    # specifying the previous edge id
    sql = """
    INSERT INTO editing_session.roads (road_code, road_type, road_class) VALUES (
        'TC002', 'road', 'Communale'
    )
    ;
    UPDATE editing_session.edges
    SET road_code = 'TC002'
    WHERE id = 7831
    ;
    SELECT
        e.id, e.road_code,
        e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative,
        e.previous_edge_id, e.next_edge_id
    FROM editing_session.edges AS e
    WHERE e.road_code IN ('T001', 'TC002')
        ORDER BY e.id
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)
    # Check the number of edges for the road
    assert len(edges) == 3
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None

    # Check the road code has been updated for the edges
    assert [edges[0][1], edges[1][1], edges[2][1]] == ["T001", "TC002", "T001"]

    # Check the previous and next edges ids have been updated
    assert [edges[0][10], edges[0][11]] == [None, 7832]
    assert [edges[1][10], edges[1][11]] == [None, None]
    assert [edges[2][10], edges[2][11]] == [7830, None]

    # Check the references have been calculated for all the edges of the road
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 0.0, 0.0]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [0, 622.67, 622.67]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 0.0, 0.0]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [0, 248.21, 248.21]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [0, 622.67, 622.67]
    assert [edges[2][7], edges[2][8], edges[2][9]] == [0, 671.07, 671.07]


def test_update_edge_and_cross_other_edge(processing_provider: Provider):
    """Update the geometry of an edge to cross another edge of anoter road
    Check the edge is cut in two edges and the attributes of the new edges and of the crossed edge are updated
    """

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Update the last edge and cut the road D138 in two edges
    sql = """
    UPDATE editing_session.edges
    SET geom = ST_SetSRID(
        ST_GeomFromText(
            'LineString (
                474709 6894739, 474677.14 6894702.57, 474588.48 6894552.78
            )'
        ),
        2154
    )
    WHERE id = 7832
    ;
    SELECT
        e.id, e.road_code,
        e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative,
        e.previous_edge_id, e.next_edge_id
    FROM editing_session.edges AS e
    WHERE e.road_code = 'T001'
    AND e.id >= 7832
    ORDER BY e.id
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)
    # Check the number of edges for the road
    assert len(edges) == 2
    assert edges[0] is not None
    assert edges[1] is not None

    # Check the previous and next edges ids have been updated
    assert [edges[0][10], edges[0][11]] == [7830, 7834]

    # Check the references have been calculated for all the edges of the road
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 622.67, 622.67]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [0, 705.01, 705.01]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 705.01, 705.01]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [0, 845.11, 845.11]

    # Check other road has been cut in two edges
    sql = """
    SELECT
            e.id, e.road_code,
            e.start_node, e.end_node,
            e.start_marker, e.start_abscissa, e.start_cumulative,
            e.end_marker, e.end_abscissa, e.end_cumulative,
            e.previous_edge_id, e.next_edge_id

    FROM editing_session.edges AS e
    WHERE
    road_code IN ('D138')
    AND (SELECT a.end_node FROM editing_session.edges AS a WHERE a.id = 7832) IN (e.start_node, e.end_node)
    ORDER BY id
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)
    # Check the number of edges for the road
    assert len(edges) == 2
    assert edges[0] is not None
    assert edges[1] is not None

    # Check the previous and next edges ids have been updated
    assert [edges[0][10], edges[0][11]] == [2264, 7833]
    assert [edges[1][10], edges[1][11]] == [2265, 2266]

    # Check references
    assert [edges[0][4], edges[0][5], edges[0][6]] == [8, 980.53, 8995.59]  # not changed
    assert [edges[0][7], edges[0][8], edges[0][9]] == [9, 740.83, 9762.14]  # changed
    assert [edges[1][4], edges[1][5], edges[1][6]] == [9, 740.83, 9762.14]  # new edge
    assert [edges[1][7], edges[1][8], edges[1][9]] == [14, 490.92, 14564.3]  # like previous edge


def test_delete_road_edge(processing_provider: Provider):
    """Delete an edge of a road and test consequences"""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Delete the edge 7832 which is in the middle of the road T001
    sql = """
    DELETE FROM editing_session.edges
    WHERE id = 7832
    ;
    SELECT
        e.id, e.road_code,
        e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative,
        e.previous_edge_id, e.next_edge_id
    FROM editing_session.edges AS e
    WHERE e.road_code = 'T001'
    ORDER BY e.id
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)
    # Check the number of edges for the road
    assert len(edges) == 2
    assert edges[0] is not None
    assert edges[1] is not None
    # Check the edges ids
    assert [edges[0][0], edges[1][0]] == [7830, 7834]
    # Check the previous and next edges ids have been updated
    assert [edges[0][10], edges[0][11]] == [None, 7834]
    assert [edges[1][10], edges[1][11]] == [7830, None]
    # Check the start and end nodes
    assert [edges[0][2], edges[0][3]] == [5758, 5760]
    assert [edges[1][2], edges[1][3]] == [5762, 5761]
    # Check the references have been calculated for all the edges of the road
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 0.0, 0.0]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [0, 622.67, 622.67]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 622.67, 622.67]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [0, 762.77, 762.77]


def test_create_roundabout(processing_provider: Provider):
    """Create a roundabout and check the result"""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Create road
    sql = """
    INSERT INTO editing_session.roads (road_code, road_type, road_class) VALUES (
        'R001', 'roundabout', 'Départementale'
    );
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    # Get the state of the crossing edges before creating the roundabout
    sql = """
    SELECT
        e.id, e.road_code,
        e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative,
        e.previous_edge_id, e.next_edge_id
    FROM editing_session.edges AS e
    WHERE e.road_code IN ('D138', 'D152')
    AND e.id IN (2497, 2498, 2264, 2265)
    ORDER BY e.id
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    edges = []
    for a in data:
        edges.append(a if a else None)

    # Check the number of edges
    assert len(edges) == 4
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None

    # Check the references of the crossing edges before creating the roundabout
    assert [edges[0][4], edges[0][5], edges[0][6]] == [8, 242.6, 8257.66]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [8, 980.53, 8995.59]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [8, 980.53, 8995.59]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [9, 740.83, 9762.14]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [4, 153.24, 4283.15]
    assert [edges[2][7], edges[2][8], edges[2][9]] == [8, 992.03, 9073.84]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [8, 992.03, 9073.84]
    assert [edges[3][7], edges[3][8], edges[3][9]] == [9, 667.32, 9792.51]

    # Create the roundabout with a circle geometry intersecting the two roads D138 and D152
    # as created by the QGIS tool "Add circular feature by center and other point"
    # The run the method finalizing the roundabout creation
    sql = """
    INSERT INTO editing_session.edges (road_code, geom) VALUES (
        'R001',
        ST_SetSRID(
            ST_GeomFromText(
                'LineString (474022.29999999998835847 6895008.47677140217274427, 474022.45580079068895429 6895008.47541926149278879, 474022.61155464593321085 6895008.47136324737221003, 474022.76721464429283515 6895008.4646045807749033, 474022.92273389274487272 6895008.45514529757201672, 474023.07806554064154625 6895008.44298824854195118, 474023.23316279373830184 6895008.42813709657639265, 474023.38797892857110128 6895008.41059631295502186, 474023.54246730625163764 6895008.39037118479609489, 474023.69658138667000458 6895008.36746780294924974, 474023.85027474246453494 6895008.34189306851476431, 474024.00350107299163938 6895008.31365468446165323, 474024.15621421835385263 6895008.2827611593529582, 474024.30836817319504917 6895008.24922179896384478, 474024.4599171006702818 6895008.21304670721292496, 474024.61081534612458199 6895008.1742467824369669, 474024.76101745112100616 6895008.13283371273428202, 474024.91047816659556702 6895008.08881997410207987, 474025.05915246717631817 6895008.04221882577985525, 474025.20699556422187015 6895007.99304430652409792, 474025.3539629194419831 6895007.94131123088300228, 474025.50001025857636705 6895007.88703518267720938, 474025.64509358425857499 6895007.83023251313716173, 474025.78916918981121853 6895007.77092033438384533, 474025.93219367187703028 6895007.70911651384085417, 474026.07412394392304122 6895007.64483967050909996, 474026.21491724898805842 6895007.57810916844755411, 474026.3545311726629734 6895007.5089451102539897, 474026.49292365572182462 6895007.43736833147704601, 474026.63005300710210577 6895007.36340039409697056, 474026.76587791607016698 6895007.28706358280032873, 474026.90035746496869251 6895007.20838089380413294, 474027.03345114132389426 6895007.12737603019922972, 474027.16511885036015883 6895007.04407339449971914, 474027.29532092669978738 6895006.95849808305501938, 474027.42401814647018909 6895006.87067587394267321, 474027.55117173935286701 6895006.78063322603702545, 474027.6767433998757042 6895006.68839726410806179, 474027.80069529911270365 6895006.5939957732334733, 474027.9229900962091051 6895006.49745719414204359, 474028.04359094944084063 6895006.3988106083124876, 474028.1624615274486132 6895006.29808573424816132, 474028.27956602000631392 6895006.1953129144385457, 474028.39486914913868532 6895006.09052310977131128, 474028.50833617930766195 6895005.98374788928776979, 474028.61993292823899537 6895005.87501941900700331, 474028.72962577705038711 6895005.76437045354396105, 474028.83738168037962168 6895005.65183432586491108, 474028.94316817651269957 6895005.53744493890553713, 474029.04695339681347832 6895005.42123675160109997, 474029.14870607573539019 6895005.30324477329850197, 474029.24839555990183726 6895005.1835045488551259, 474029.34599181753583252 6895005.06205215025693178, 474029.4414654474821873 6895004.93892416451126337, 474029.53478768799686804 6895004.81415768712759018, 474029.62593042547814548 6895004.68779030162841082, 474029.71486620284849778 6895004.55986007768660784, 474029.80156822787830606 6895004.43040555436164141, 474029.88601038139313459 6895004.29946573078632355, 474029.96816722489893436 6895004.1670800531283021, 474030.04801400832366198 6895004.03328840248286724, 474030.1255266775842756 6895003.89813108369708061, 474030.20068188180448487 6895003.7616488141939044, 474030.27345698018325493 6895003.62388270907104015, 474030.34383004903793335 6895003.48487427085638046, 474030.41177988814888522 6895003.34466537646949291, 474030.47728602751158178 6895003.20329826418310404, 474030.54032873315736651 6895003.06081552151590586, 474030.60088901326525956 6895002.91726007219403982, 474030.65894862386630848 6895002.7726751621812582, 474030.71449007431510836 6895002.62710434757173061, 474030.76749663252849132 6895002.48059148341417313, 474030.81795233016600832 6895002.33318070601671934, 474030.86584196728654206 6895002.18491642363369465, 474030.91115111688850448 6895002.03584330156445503, 474030.95386612956644967 6895001.88600624911487103, 474030.99397413723636419 6895001.73545040469616652, 474031.03146305726841092 6895001.5842211227864027, 474031.06632159592118114 6895001.43236396368592978, 474031.09853925200877711 6895001.27992467302829027, 474031.1281063198694028 6895001.12694917432963848, 474031.15501389227574691 6895000.9734835522249341, 474031.17925386328715831 6895000.81957403849810362, 474031.20081893051974475 6895000.66526699811220169, 474031.21970259735826403 6895000.51060891803354025, 474031.23589917516801506 6895000.35564638860523701, 474031.24940378457540646 6895000.20042609330266714, 474031.26021235733060166 6895000.04499479196965694, 474031.2683216372388415 6894999.88939930871129036, 474031.27372918144101277 6894999.73368651792407036, 474031.27643336087930948 6894999.57790332939475775, 474031.27643336087930948 6894999.42209667060524225, 474031.27372918144101277 6894999.26631348207592964, 474031.2683216372388415 6894999.11060069128870964, 474031.26021235733060166 6894998.95500520803034306, 474031.24940378457540646 6894998.79957390669733286, 474031.23589917516801506 6894998.64435361139476299, 474031.21970259735826403 6894998.48939108196645975, 474031.20081893051974475 6894998.33473300188779831, 474031.17925386328715831 6894998.18042596150189638, 474031.15501389227574691 6894998.0265164477750659, 474031.1281063198694028 6894997.87305082567036152, 474031.09853925200877711 6894997.72007532697170973, 474031.06632159592118114 6894997.56763603631407022, 474031.03146305726841092 6894997.4157788772135973, 474030.99397413723636419 6894997.26454959530383348, 474030.95386612956644967 6894997.11399375088512897, 474030.91115111688850448 6894996.96415669843554497, 474030.86584196728654206 6894996.81508357636630535, 474030.81795233016600832 6894996.66681929398328066, 474030.76749663252849132 6894996.51940851658582687, 474030.71449007431510836 6894996.37289565242826939, 474030.65894862386630848 6894996.2273248378187418, 474030.60088901326525956 6894996.08273992780596018, 474030.54032873315736651 6894995.93918447848409414, 474030.47728602751158178 6894995.79670173581689596, 474030.41177988814888522 6894995.65533462353050709, 474030.34383004903793335 6894995.51512572914361954, 474030.27345698018325493 6894995.37611729092895985, 474030.20068188180448487 6894995.2383511858060956, 474030.1255266775842756 6894995.10186891630291939, 474030.04801400832366198 6894994.96671159751713276, 474029.96816722489893436 6894994.8329199468716979, 474029.88601038139313459 6894994.70053426921367645, 474029.80156822787830606 6894994.56959444563835859, 474029.71486620284849778 6894994.44013992231339216, 474029.62593042547814548 6894994.31220969837158918, 474029.53478768799686804 6894994.18584231287240982, 474029.4414654474821873 6894994.06107583548873663, 474029.34599181753583252 6894993.93794784974306822, 474029.24839555990183726 6894993.8164954511448741, 474029.14870607573539019 6894993.69675522670149803, 474029.04695339681347832 6894993.57876324839890003, 474028.94316817651269957 6894993.46255506109446287, 474028.83738168037962168 6894993.34816567413508892, 474028.72962577705038711 6894993.23562954645603895, 474028.61993292823899537 6894993.12498058099299669, 474028.50833617930766195 6894993.01625211071223021, 474028.39486914913868532 6894992.90947689022868872, 474028.27956602000631392 6894992.8046870855614543, 474028.1624615274486132 6894992.70191426575183868, 474028.04359094944084063 6894992.6011893916875124, 474027.9229900962091051 6894992.50254280585795641, 474027.80069529911270365 6894992.4060042267665267, 474027.6767433998757042 6894992.31160273589193821, 474027.55117173935286701 6894992.21936677396297455, 474027.42401814647018909 6894992.12932412605732679, 474027.29532092669978738 6894992.04150191694498062, 474027.16511885036015883 6894991.95592660550028086, 474027.03345114132389426 6894991.87262396980077028, 474026.90035746496869251 6894991.79161910619586706, 474026.76587791607016698 6894991.71293641719967127, 474026.63005300710210577 6894991.63659960590302944, 474026.49292365572182462 6894991.56263166852295399, 474026.3545311726629734 6894991.4910548897460103, 474026.21491724898805842 6894991.42189083155244589, 474026.07412394392304122 6894991.35516032949090004, 474025.93219367187703028 6894991.29088348615914583, 474025.78916918981121853 6894991.22907966561615467, 474025.64509358425857499 6894991.16976748686283827, 474025.50001025857636705 6894991.11296481732279062, 474025.3539629194419831 6894991.05868876911699772, 474025.20699556422187015 6894991.00695569347590208, 474025.05915246717631817 6894990.95778117422014475, 474024.91047816659556702 6894990.91118002589792013, 474024.76101745112100616 6894990.86716628726571798, 474024.61081534612458199 6894990.8257532175630331, 474024.4599171006702818 6894990.78695329278707504, 474024.30836817319504917 6894990.75077820103615522, 474024.15621421835385263 6894990.7172388406470418, 474024.00350107299163938 6894990.68634531553834677, 474023.85027474246453494 6894990.65810693148523569, 474023.69658138667000458 6894990.63253219705075026, 474023.54246730625163764 6894990.60962881520390511, 474023.38797892857110128 6894990.58940368704497814, 474023.23316279373830184 6894990.57186290342360735, 474023.07806554064154625 6894990.55701175145804882, 474022.92273389274487272 6894990.54485470242798328, 474022.76721464429283515 6894990.5353954192250967, 474022.61155464593321085 6894990.52863675262778997, 474022.45580079068895429 6894990.52458073850721121, 474022.29999999998835847 6894990.52322859782725573, 474022.14419920928776264 6894990.52458073850721121, 474021.98844535404350609 6894990.52863675262778997, 474021.83278535568388179 6894990.5353954192250967, 474021.67726610723184422 6894990.54485470242798328, 474021.52193445933517069 6894990.55701175145804882, 474021.36683720623841509 6894990.57186290342360735, 474021.21202107140561566 6894990.58940368704497814, 474021.0575326937250793 6894990.60962881520390511, 474020.90341861330671236 6894990.63253219705075026, 474020.749725257512182 6894990.65810693148523569, 474020.59649892698507756 6894990.68634531553834677, 474020.44378578162286431 6894990.7172388406470418, 474020.29163182678166777 6894990.75077820103615522, 474020.14008289930643514 6894990.78695329278707504, 474019.98918465385213494 6894990.8257532175630331, 474019.83898254885571077 6894990.86716628726571798, 474019.68952183338114992 6894990.91118002589792013, 474019.54084753280039877 6894990.95778117422014475, 474019.39300443575484678 6894991.00695569347590208, 474019.24603708053473383 6894991.05868876911699772, 474019.09998974140034989 6894991.11296481732279062, 474018.95490641571814194 6894991.16976748686283827, 474018.81083081016549841 6894991.22907966561615467, 474018.66780632809968665 6894991.29088348615914583, 474018.52587605605367571 6894991.35516032949090004, 474018.38508275098865852 6894991.42189083155244589, 474018.24546882731374353 6894991.4910548897460103, 474018.10707634425489232 6894991.56263166852295399, 474017.96994699287461117 6894991.63659960590302944, 474017.83412208390654996 6894991.71293641719967127, 474017.69964253500802442 6894991.79161910619586706, 474017.56654885865282267 6894991.87262396980077028, 474017.4348811496165581 6894991.95592660550028086, 474017.30467907327692956 6894992.04150191694498062, 474017.17598185350652784 6894992.12932412605732679, 474017.04882826062384993 6894992.21936677396297455, 474016.92325660010101274 6894992.31160273589193821, 474016.79930470086401328 6894992.4060042267665267, 474016.67700990376761183 6894992.50254280585795641, 474016.5564090505358763 6894992.6011893916875124, 474016.43753847252810374 6894992.70191426575183868, 474016.32043397997040302 6894992.8046870855614543, 474016.20513085083803162 6894992.90947689022868872, 474016.09166382066905499 6894993.01625211071223021, 474015.98006707173772156 6894993.12498058099299669, 474015.87037422292632982 6894993.23562954645603895, 474015.76261831959709525 6894993.34816567413508892, 474015.65683182346401736 6894993.46255506109446287, 474015.55304660316323861 6894993.57876324839890003, 474015.45129392424132675 6894993.69675522670149803, 474015.35160444007487968 6894993.8164954511448741, 474015.25400818244088441 6894993.93794784974306822, 474015.15853455249452963 6894994.06107583548873663, 474015.06521231197984889 6894994.18584231287240982, 474014.97406957449857146 6894994.31220969837158918, 474014.88513379712821916 6894994.44013992231339216, 474014.79843177209841087 6894994.56959444563835859, 474014.71398961858358234 6894994.70053426921367645, 474014.63183277507778257 6894994.8329199468716979, 474014.55198599165305495 6894994.96671159751713276, 474014.47447332239244133 6894995.10186891630291939, 474014.39931811817223206 6894995.2383511858060956, 474014.32654301979346201 6894995.37611729092895985, 474014.25616995093878359 6894995.51512572914361954, 474014.18822011182783172 6894995.65533462353050709, 474014.12271397246513516 6894995.79670173581689596, 474014.05967126681935042 6894995.93918447848409414, 474013.99911098671145737 6894996.08273992780596018, 474013.94105137611040846 6894996.2273248378187418, 474013.88550992566160858 6894996.37289565242826939, 474013.83250336744822562 6894996.51940851658582687, 474013.78204766981070861 6894996.66681929398328066, 474013.73415803269017488 6894996.81508357636630535, 474013.68884888308821246 6894996.96415669843554497, 474013.64613387041026726 6894997.11399375088512897, 474013.60602586274035275 6894997.26454959530383348, 474013.56853694270830601 6894997.4157788772135973, 474013.53367840405553579 6894997.56763603631407022, 474013.50146074796793982 6894997.72007532697170973, 474013.47189368010731414 6894997.87305082567036152, 474013.44498610770097002 6894998.0265164477750659, 474013.42074613668955863 6894998.18042596150189638, 474013.39918106945697218 6894998.33473300188779831, 474013.38029740261845291 6894998.48939108196645975, 474013.36410082480870187 6894998.64435361139476299, 474013.35059621540131047 6894998.79957390669733286, 474013.33978764264611527 6894998.95500520803034306, 474013.33167836273787543 6894999.11060069128870964, 474013.32627081853570417 6894999.26631348207592964, 474013.32356663909740746 6894999.42209667060524225, 474013.32356663909740746 6894999.57790332939475775, 474013.32627081853570417 6894999.73368651792407036, 474013.33167836273787543 6894999.88939930871129036, 474013.33978764264611527 6895000.04499479196965694, 474013.35059621540131047 6895000.20042609330266714, 474013.36410082480870187 6895000.35564638860523701, 474013.38029740261845291 6895000.51060891803354025, 474013.39918106945697218 6895000.66526699811220169, 474013.42074613668955863 6895000.81957403849810362, 474013.44498610770097002 6895000.9734835522249341, 474013.47189368010731414 6895001.12694917432963848, 474013.50146074796793982 6895001.27992467302829027, 474013.53367840405553579 6895001.43236396368592978, 474013.56853694270830601 6895001.5842211227864027, 474013.60602586274035275 6895001.73545040469616652, 474013.64613387041026726 6895001.88600624911487103, 474013.68884888308821246 6895002.03584330156445503, 474013.73415803269017488 6895002.18491642363369465, 474013.78204766981070861 6895002.33318070601671934, 474013.83250336744822562 6895002.48059148341417313, 474013.88550992566160858 6895002.62710434757173061, 474013.94105137611040846 6895002.7726751621812582, 474013.99911098671145737 6895002.91726007219403982, 474014.05967126681935042 6895003.06081552151590586, 474014.12271397246513516 6895003.20329826418310404, 474014.18822011182783172 6895003.34466537646949291, 474014.25616995093878359 6895003.48487427085638046, 474014.32654301979346201 6895003.62388270907104015, 474014.39931811817223206 6895003.7616488141939044, 474014.47447332239244133 6895003.89813108369708061, 474014.55198599165305495 6895004.03328840248286724, 474014.63183277507778257 6895004.1670800531283021, 474014.71398961858358234 6895004.29946573078632355, 474014.79843177209841087 6895004.43040555436164141, 474014.88513379712821916 6895004.55986007768660784, 474014.97406957449857146 6895004.68779030162841082, 474015.06521231197984889 6895004.81415768712759018, 474015.15853455249452963 6895004.93892416451126337, 474015.25400818244088441 6895005.06205215025693178, 474015.35160444007487968 6895005.1835045488551259, 474015.45129392424132675 6895005.30324477329850197, 474015.55304660316323861 6895005.42123675160109997, 474015.65683182346401736 6895005.53744493890553713, 474015.76261831959709525 6895005.65183432586491108, 474015.87037422292632982 6895005.76437045354396105, 474015.98006707173772156 6895005.87501941900700331, 474016.09166382066905499 6895005.98374788928776979, 474016.20513085083803162 6895006.09052310977131128, 474016.32043397997040302 6895006.1953129144385457, 474016.43753847252810374 6895006.29808573424816132, 474016.5564090505358763 6895006.3988106083124876, 474016.67700990376761183 6895006.49745719414204359, 474016.79930470086401328 6895006.5939957732334733, 474016.92325660010101274 6895006.68839726410806179, 474017.04882826062384993 6895006.78063322603702545, 474017.17598185350652784 6895006.87067587394267321, 474017.30467907327692956 6895006.95849808305501938, 474017.4348811496165581 6895007.04407339449971914, 474017.56654885865282267 6895007.12737603019922972, 474017.69964253500802442 6895007.20838089380413294, 474017.83412208390654996 6895007.28706358280032873, 474017.96994699287461117 6895007.36340039409697056, 474018.10707634425489232 6895007.43736833147704601, 474018.24546882731374353 6895007.5089451102539897, 474018.38508275098865852 6895007.57810916844755411, 474018.52587605605367571 6895007.64483967050909996, 474018.66780632809968665 6895007.70911651384085417, 474018.81083081016549841 6895007.77092033438384533, 474018.95490641571814194 6895007.83023251313716173, 474019.09998974140034989 6895007.88703518267720938, 474019.24603708053473383 6895007.94131123088300228, 474019.39300443575484678 6895007.99304430652409792, 474019.54084753280039877 6895008.04221882577985525, 474019.68952183338114992 6895008.08881997410207987, 474019.83898254885571077 6895008.13283371273428202, 474019.98918465385213494 6895008.1742467824369669, 474020.14008289930643514 6895008.21304670721292496, 474020.29163182678166777 6895008.24922179896384478, 474020.44378578162286431 6895008.2827611593529582, 474020.59649892698507756 6895008.31365468446165323, 474020.749725257512182 6895008.34189306851476431, 474020.90341861330671236 6895008.36746780294924974, 474021.0575326937250793 6895008.39037118479609489, 474021.21202107140561566 6895008.41059631295502186, 474021.36683720623841509 6895008.42813709657639265, 474021.52193445933517069 6895008.44298824854195118, 474021.67726610723184422 6895008.45514529757201672, 474021.83278535568388179 6895008.4646045807749033, 474021.98844535404350609 6895008.47136324737221003, 474022.14419920928776264 6895008.47541926149278879, 474022.29999999998835847 6895008.47677140217274427)'
            ),
            2154
        )
    )
    ;
    SELECT editing_session.clean_digitized_roundabout('R001') AS returned_result
    ;
    SELECT
        e.id, e.road_code,
        e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative,
        e.previous_edge_id, e.next_edge_id
    FROM editing_session.edges AS e
    WHERE e.road_code IN ('D138', 'D152')
    AND ST_Intersects(
        e.geom,
        (SELECT ST_Collect(geom) FROM editing_session.edges WHERE road_code = 'R001')
    )
    ORDER BY e.id
    ;
    """  # noqa: E501
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    edges = []
    for a in data:
        edges.append(a if a else None)

    # Check the number of edges
    assert len(edges) == 4
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None

    # Check the ids
    assert [edges[0][0], edges[1][0], edges[2][0], edges[3][0]] == [2264, 2497, 7836, 7837]

    # Check the references of the edges
    assert [edges[0][4], edges[0][5], edges[0][6]] == [8, 242.6, 8257.66]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [8, 971.52, 8986.58]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [4, 153.24, 4283.15]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [8, 983.02, 9064.82]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [8, 983.02, 9064.82]
    assert [edges[2][7], edges[2][8], edges[2][9]] == [9, 667.35, 9774.5]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [8, 971.52, 8986.58]
    assert [edges[3][7], edges[3][8], edges[3][9]] == [9, 740.82, 9744.13]

    # Check the marker 0 has been created
    sql = """
    SELECT
        id, road_code, code, abscissa,
        ST_X(geom), ST_Y(geom)
    FROM editing_session.markers
    WHERE road_code  = 'R001'
    AND code = 0
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    markers = []
    for a in data:
        markers.append(a if a else None)
    assert len(markers) == 1
    assert markers is not None
    assert markers[0] is not None
    assert markers[0][0] == 8340
    assert markers[0][1] == "R001"
    assert markers[0][2] == 0
    assert markers[0][3] == 0
    assert markers[0][4] == 474017.3
    assert markers[0][5] == 6895007.0

    # Test the edges data of the newly created roundabout
    sql = """
    SELECT
        id, road_code,
        start_node, end_node,
        start_marker, start_abscissa, start_cumulative,
        end_marker, end_abscissa, end_cumulative,
        previous_edge_id, next_edge_id
    FROM editing_session.edges
    WHERE road_code = 'R001'
    ORDER BY start_cumulative
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)
    assert len(edges) == 4
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None

    # Check the ids of the edges of the roundabout
    assert [edges[0][0], edges[1][0], edges[2][0], edges[3][0]] == [7840, 7841, 7838, 7835]

    # Check the references of the edges of the roundabout
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 0, 0]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [0, 22.45, 22.45]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 22.45, 22.45]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [0, 30.52, 30.52]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [0, 30.52, 30.52]
    assert [edges[2][7], edges[2][8], edges[2][9]] == [0, 51.18, 51.18]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [0, 51.18, 51.18]
    assert [edges[3][7], edges[3][8], edges[3][9]] == [0, 56.46, 56.46]

    # Check previous_edge_id and next_edge_id of the roundabout edges
    assert [edges[0][10], edges[0][11]] == [None, 7841]
    assert [edges[1][10], edges[1][11]] == [7840, 7838]
    assert [edges[2][10], edges[2][11]] == [7841, 7835]
    assert [edges[3][10], edges[3][11]] == [7838, None]


def test_move_road_marker():
    """Move a marker and see if the edges are correctly updated."""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Get the edges data before moving the marker
    sql = """
    SELECT
        e.id,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative
    FROM editing_session.edges AS e
    WHERE road_code IN ('D138')
    AND start_cumulative > 7930
    ORDER BY start_cumulative
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    edges = []
    for a in data:
        edges.append(a if a else None)
    assert len(edges) == 7
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None
    assert edges[4] is not None
    assert edges[5] is not None
    assert edges[6] is not None

    # Data before
    #   id  | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative
    # ------+--------------+----------------+------------------+------------+--------------+---------------
    #  2260 |            7 |         836.84 |          7930.99 |          8 |        242.6 |        8257.66
    #  2264 |            8 |          242.6 |          8257.66 |          8 |       971.52 |        8986.58
    #  7837 |            8 |         971.52 |          8986.58 |          9 |       740.82 |        9744.13
    #  7833 |            9 |         740.82 |          9744.13 |         14 |       490.92 |        14546.3
    #  2266 |           14 |         490.92 |          14546.3 |         16 |       778.88 |          16857
    #  4506 |           16 |         778.88 |            16857 |         16 |       813.14 |        16891.3
    #  2267 |           16 |         813.14 |          16891.3 |         19 |       334.71 |        19428.7

    # Check the references of the edges before moving the marker
    assert [edges[0][1], edges[0][2], edges[0][3]] == [7, 836.84, 7930.99]
    assert [edges[0][4], edges[0][5], edges[0][6]] == [8, 242.6, 8257.66]
    assert [edges[1][1], edges[1][2], edges[1][3]] == [8, 242.6, 8257.66]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [8, 971.52, 8986.58]
    assert [edges[2][1], edges[2][2], edges[2][3]] == [8, 971.52, 8986.58]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [9, 740.82, 9744.13]
    assert [edges[3][1], edges[3][2], edges[3][3]] == [9, 740.82, 9744.13]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [14, 490.92, 14546.3]

    # Move the marker 8 of the road D138 by 60 meters southwards
    sql_update = """
    UPDATE editing_session.markers
    SET geom = ST_Translate(geom, 0, -60)
    WHERE road_code = 'D138' AND code = 8
    ;
    """
    try:
        data = connection.executeSql(sql_update)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    # Get the edges data after moving the marker
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    edges = []
    for a in data:
        edges.append(a if a else None)

    assert len(edges) == 7
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None
    assert edges[4] is not None
    assert edges[5] is not None
    assert edges[6] is not None

    # Data after
    #   id  | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative
    # ------+--------------+----------------+------------------+------------+--------------+---------------
    #  2260 |            7 |         836.84 |          7930.99 |          8 |       177.91 |        8257.66
    #  2264 |            8 |         177.91 |          8257.66 |          8 |       906.83 |        8986.58
    #  7837 |            8 |         906.83 |          8986.58 |          9 |       740.82 |        9744.13
    #  7833 |            9 |         740.82 |          9744.13 |         14 |       490.92 |        14546.3
    #  2266 |           14 |         490.92 |          14546.3 |         16 |       778.88 |          16857
    #  4506 |           16 |         778.88 |            16857 |         16 |       813.14 |        16891.3
    #  2267 |           16 |         813.14 |          16891.3 |         19 |       334.71 |        19428.7

    # Check the references of the edges after moving the marker
    assert [edges[0][1], edges[0][2], edges[0][3]] == [7, 836.84, 7930.99]
    assert [edges[0][4], edges[0][5], edges[0][6]] == [8, 177.91, 8257.66]
    assert [edges[1][1], edges[1][2], edges[1][3]] == [8, 177.91, 8257.66]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [8, 906.83, 8986.58]
    assert [edges[2][1], edges[2][2], edges[2][3]] == [8, 906.83, 8986.58]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [9, 740.82, 9744.13]
    assert [edges[3][1], edges[3][2], edges[3][3]] == [9, 740.82, 9744.13]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [14, 490.92, 14546.3]


def test_delete_road_marker():
    """Delete markers of a road and see if the edges are correctly updated."""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Delete markers 8, 16 and 19 of the road D138
    sql_delete = """
    DELETE FROM editing_session.markers
    WHERE road_code = 'D138' AND code IN (8, 16, 19)
    ;
    """
    try:
        data = connection.executeSql(sql_delete)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    # Get the edges data after deleting the markers
    sql = """
    SELECT
        e.id,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative
    FROM editing_session.edges AS e
    WHERE road_code IN ('D138')
    AND start_cumulative > 7930
    ORDER BY start_cumulative
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)
    assert len(edges) == 7
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None
    assert edges[4] is not None
    assert edges[5] is not None
    assert edges[6] is not None

    # Data after
    #   id  | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative
    # ------+--------------+----------------+------------------+------------+--------------+----------------
    #  2260 |            7 |         836.84 |          7930.99 |          7 |       1163.5 |        8257.66
    #  2264 |            7 |         1163.5 |          8257.66 |          7 |      1892.42 |        8986.58
    #  7837 |            7 |        1892.42 |          8986.58 |          9 |       740.82 |        9744.13
    #  7833 |            9 |         740.82 |          9744.13 |         14 |       490.92 |        14546.3
    #  2266 |           14 |         490.92 |          14546.3 |         15 |      1806.27 |          16857
    #  4506 |           15 |        1806.27 |            16857 |         15 |      1840.53 |        16891.3
    #  2267 |           15 |        1840.53 |          16891.3 |         18 |      1341.54 |        19428.7

    # Check the references of the edges after deleting the markers
    assert [edges[0][1], edges[0][2], edges[0][3]] == [7, 836.84, 7930.99]
    assert [edges[0][4], edges[0][5], edges[0][6]] == [7, 1163.5, 8257.66]
    assert [edges[1][1], edges[1][2], edges[1][3]] == [7, 1163.5, 8257.66]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [7, 1892.42, 8986.58]
    assert [edges[2][1], edges[2][2], edges[2][3]] == [7, 1892.42, 8986.58]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [9, 740.82, 9744.13]
    assert [edges[3][1], edges[3][2], edges[3][3]] == [9, 740.82, 9744.13]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [14, 490.92, 14546.3]
    assert [edges[4][1], edges[4][2], edges[4][3]] == [14, 490.92, 14546.3]
    assert [edges[4][4], edges[4][5], edges[4][6]] == [15, 1806.27, 16857]
    assert [edges[5][1], edges[5][2], edges[5][3]] == [15, 1806.27, 16857]
    assert [edges[5][4], edges[5][5], edges[5][6]] == [15, 1840.53, 16891.3]
    assert [edges[6][1], edges[6][2], edges[6][3]] == [15, 1840.53, 16891.3]
    assert [edges[6][4], edges[6][5], edges[6][6]] == [18, 1341.54, 19428.7]


def test_add_road_marker():
    """
    Add a marker and see if the edges are correctly updated.
    """

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Get the edges data before moving the marker
    sql = """
    SELECT
        e.id,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative
    FROM editing_session.edges AS e
    WHERE road_code IN ('D138')
    AND start_cumulative > 7930
    ORDER BY start_cumulative
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    edges = []
    for a in data:
        edges.append(a if a else None)
    assert len(edges) == 7
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None

    # Data before
    #   id  | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative
    # ------+--------------+----------------+------------------+------------+--------------+----------------
    #  2260 |            7 |         836.84 |          7930.99 |          7 |       1163.5 |        8257.66
    #  2264 |            7 |         1163.5 |          8257.66 |          7 |      1892.42 |        8986.58
    #  7837 |            7 |        1892.42 |          8986.58 |          9 |       740.82 |        9744.13
    #  7833 |            9 |         740.82 |          9744.13 |         14 |       490.92 |        14546.3
    #  2266 |           14 |         490.92 |          14546.3 |         15 |      1806.27 |          16857
    #  4506 |           15 |        1806.27 |            16857 |         15 |      1840.53 |        16891.3
    #  2267 |           15 |        1840.53 |          16891.3 |         18 |      1341.54 |        19428.7

    # Check the references of the edges before adding the marker
    assert [edges[0][1], edges[0][2], edges[0][3]] == [7, 836.84, 7930.99]
    assert [edges[0][4], edges[0][5], edges[0][6]] == [7, 1163.5, 8257.66]
    assert [edges[1][1], edges[1][2], edges[1][3]] == [7, 1163.5, 8257.66]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [7, 1892.42, 8986.58]
    assert [edges[2][1], edges[2][2], edges[2][3]] == [7, 1892.42, 8986.58]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [9, 740.82, 9744.13]
    assert [edges[3][1], edges[3][2], edges[3][3]] == [9, 740.82, 9744.13]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [14, 490.92, 14546.3]

    # Add the marker 8 for the road D138
    sql_insert = """
    INSERT INTO editing_session.markers (geom, road_code, code, abscissa) VALUES (
        ST_SetSRID(
            ST_GeomFromText(
                'POINT (473524 6895644)'
            ),
            2154
        ),
        'D138', 8, 0
    )
    ;
    """
    try:
        data = connection.executeSql(sql_insert)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    # Get the edges data after adding the marker
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    edges = []
    for a in data:
        edges.append(a if a else None)

    assert len(edges) == 7
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None

    # Data after
    #   id  | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative
    # ------+--------------+----------------+------------------+------------+--------------+----------------
    #  2260 |            7 |         836.84 |          7930.99 |          8 |       202.78 |        8257.66
    #  2264 |            8 |         202.78 |          8257.66 |          8 |        931.7 |        8986.58
    #  7837 |            8 |          931.7 |          8986.58 |          9 |       740.82 |        9744.13
    #  7833 |            9 |         740.82 |          9744.13 |         14 |       490.92 |        14546.3
    #  2266 |           14 |         490.92 |          14546.3 |         15 |      1806.27 |          16857
    #  4506 |           15 |        1806.27 |            16857 |         15 |      1840.53 |        16891.3
    #  2267 |           15 |        1840.53 |          16891.3 |         18 |      1341.54 |        19428.7

    # Check the references of the edges after moving the marker
    assert [edges[0][1], edges[0][2], edges[0][3]] == [7, 836.84, 7930.99]
    assert [edges[0][4], edges[0][5], edges[0][6]] == [8, 202.78, 8257.66]
    assert [edges[1][1], edges[1][2], edges[1][3]] == [8, 202.78, 8257.66]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [8, 931.7, 8986.58]
    assert [edges[2][1], edges[2][2], edges[2][3]] == [8, 931.7, 8986.58]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [9, 740.82, 9744.13]
    assert [edges[3][1], edges[3][2], edges[3][3]] == [9, 740.82, 9744.13]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [14, 490.92, 14546.3]


def test_delete_road_edge_which_ends_on_roundabout_marker_0(processing_provider: Provider):
    """Delete an edge which ends on the marker 0 of a roundabout"""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Check the values of the edges of the road R001
    sql = """
    SELECT
            e.id,
            e.start_marker, e.start_abscissa, e.start_cumulative,
            e.end_marker, e.end_abscissa, e.end_cumulative
    FROM editing_session.edges AS e
    WHERE road_code IN ('R001')
    ORDER BY start_cumulative
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)

    # Data before deleting the edge
    #   id  | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative
    # ------+--------------+----------------+------------------+------------+--------------+----------------
    #  7840 |            0 |              0 |                0 |          0 |        22.45 |          22.45
    #  7841 |            0 |          22.45 |            22.45 |          0 |        30.52 |          30.52
    #  7838 |            0 |          30.52 |            30.52 |          0 |        51.18 |          51.18
    #  7835 |            0 |          51.18 |            51.18 |          0 |        56.46 |          56.46

    assert len(edges) == 4
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None
    assert edges[3] is not None

    # Check the references of the edges before deleting the edge
    assert [edges[0][1], edges[0][2], edges[0][3]] == [0, 0, 0]
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 22.45, 22.45]
    assert [edges[1][1], edges[1][2], edges[1][3]] == [0, 22.45, 22.45]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 30.52, 30.52]
    assert [edges[2][1], edges[2][2], edges[2][3]] == [0, 30.52, 30.52]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [0, 51.18, 51.18]
    assert [edges[3][1], edges[3][2], edges[3][3]] == [0, 51.18, 51.18]
    assert [edges[3][4], edges[3][5], edges[3][6]] == [0, 56.46, 56.46]

    # Delete the edge of the road D138 which ends on the marker 0 of the roundabout R001
    sql_delete = """
    DELETE FROM editing_session.edges AS e
    WHERE e.road_code = 'D138'
    AND ST_DWithin(
        ST_EndPoint(geom),
        (
            SELECT geom
            FROM editing_session.markers
            WHERE road_code = 'R001' AND code = 0
        ),
        0.10
    )
    ;
    """
    try:
        connection.executeSql(sql_delete)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    # Check the values of the edges of the road R001 after deleting the edge
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))
    edges = []
    for a in data:
        edges.append(a if a else None)

    # Data after deleting the edge
    #   id  | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative
    # ------+--------------+----------------+------------------+------------+--------------+----------------
    #  7835 |            0 |              0 |                0 |          0 |        27.74 |          27.74
    #  7841 |            0 |          27.74 |            27.74 |          0 |         35.8 |           35.8
    #  7838 |            0 |           35.8 |             35.8 |          0 |        56.46 |          56.46

    assert len(edges) == 3
    assert edges[0] is not None
    assert edges[1] is not None
    assert edges[2] is not None

    # Check the references of the edges after deleting the edge
    assert [edges[0][1], edges[0][2], edges[0][3]] == [0, 0, 0]
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 27.74, 27.74]
    assert [edges[1][1], edges[1][2], edges[1][3]] == [0, 27.74, 27.74]
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 35.8, 35.8]
    assert [edges[2][1], edges[2][2], edges[2][3]] == [0, 35.8, 35.8]
    assert [edges[2][4], edges[2][5], edges[2][6]] == [0, 56.46, 56.46]


def test_get_road_point_from_reference():
    pass


def test_get_road_substring_from_references():
    """Test the function get_road_substring_from_references which returns a substring of a road between two references."""

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = "test"
    connection = metadata.findConnection(connection_name)

    # Check the value of the geometry returned by the function get_road_substring_from_references
    # for a big road with many edges and roundabouts on the way
    sql = """
    SELECT md5(
        ST_AsText(
            (road_graph.get_road_substring_from_references(
                'D613',
                42,
                120,
                44,
                10,
                3.25,
                'right'
            )->>'geom')::geometry(MULTILINESTRING, 2154)::text
        )
    )
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    result = None
    for a in data:
        result = a if a else None

    assert result is not None
    assert result[0] == "39adeaf34ed4b7f0b6d391eed5755f93"

    # Same test for a roundabout
    # TODO : il we pass a max value bigger than the roundabout length,
    # the function should return the whole roundabout and not an empty geometry
    sql = """
    SELECT md5(
        ST_AsText(
            (editing_session.get_road_substring_from_references(
                'R001',
                0,
                10,
                0,
                30,
                2,
                'right'
            )->>'geom')::geometry(MULTILINESTRING, 2154)::text
        )
    )
    ;
    """
    try:
        data = connection.executeSql(sql)
    except QgsProviderConnectionException as e:
        raise QgsProcessingException(str(e))

    result = None
    for a in data:
        result = a if a else None

    assert result is not None
    assert result[0] == "124fc586378a3f996e514e65359d91b0"






def test_merge_editing_session_data():
    pass


def test_update_managed_objects_on_graph_change():
    pass
