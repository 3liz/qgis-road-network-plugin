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


def test_reset_test_data(
    processing_provider: Provider,
    data: Path
):
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
    connection_name = 'test'
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


def test_create_editing_session(
    processing_provider: Provider
):

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = 'test'
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


def test_create_edge(
    processing_provider: Provider
):
    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = 'test'
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
    assert [edge[0], edge[1]] == [7830, 'T001']

    # Check 2 new nodes have been created
    assert [edge[2], edge[3]] == [5758, 5759]

    # Check the references have been calculated for this new edge
    assert [edge[4], edge[5], edge[6]] == [0, 0.0, 0.0]
    assert [edge[7], edge[8], edge[9]] == [0, 870.88, 870.88]


def test_cut_edge_by_node(
    processing_provider: Provider
):
    # Add a node in the middle of the new edge
    # which should cut the new edge in two parts

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = 'test'
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

    # edge 1
    # id and road code
    assert [edges[0][0], edges[0][1]] == [7830, 'T001']
    # Check the start and end nodes
    assert [edges[0][2], edges[0][3]] == [5758, 5760]
    # Check the references have been calculated for this edge
    assert [edges[0][4], edges[0][5], edges[0][6]] == [0, 0.0, 0.0]
    assert [edges[0][7], edges[0][8], edges[0][9]] == [0, 622.67, 622.67]
    # Check the previous and next edges ids
    assert [edges[0][10], edges[0][11]] == [None, 7831]

    # edge 2
    # id and road code
    assert [edges[1][0], edges[1][1]] == [7831, 'T001']
    # Check the start and end nodes
    assert [edges[1][2], edges[1][3]] == [5760, 5759]
    # Check the references have been calculated for this edge
    assert [edges[1][4], edges[1][5], edges[1][6]] == [0, 622.67, 622.67]
    assert [edges[1][7], edges[1][8], edges[1][9]] == [0, 870.88, 870.88]
    # Check the previous and next edges ids
    assert [edges[1][10], edges[1][11]] == [7830, None]


def test_insert_edge_at_the_end_of_a_road(
    processing_provider: Provider
):

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = 'test'
    connection = metadata.findConnection(connection_name)

    # Insert a new edge at the end (touching) of the last edge
    # specifying the previous edge id
    sql = """
    INSERT INTO editing_session.edges (
        geom, road_code
        --, previous_edge_id
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
        --,
        --7831
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


def test_change_edge_road(
    processing_provider: Provider
):
    # Create a new road and change the middle edge to be part of this new road
    # Check the references have been calculated for the edge and the new road
    # And the old road

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = 'test'
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

    # Check the road code has been updated for the edges
    assert [edges[0][1], edges[1][1], edges[2][1]] == ['T001', 'TC002', 'T001']

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


def test_update_edge_and_cross_other_edge(
    processing_provider: Provider
):
    # Update the geometry of an edge to touch another edge of anoter road

    # Get PostgreSQL connection
    metadata = QgsProviderRegistry.instance().providerMetadata("postgres")
    connection_name = 'test'
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
            e.previous_edge_id, e.next_edge_id,
    e.updated_at, e.created_at

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

    # Check the previous and next edges ids have been updated
    assert [edges[0][10], edges[0][11]] == [2264, 7833]
    assert [edges[1][10], edges[1][11]] == [2265, 2266]

    # Check references
    assert [edges[0][4], edges[0][5], edges[0][6]] == [8, 980.53, 8995.59]  # not changed
    assert [edges[0][7], edges[0][8], edges[0][9]] == [9, 740.83, 9762.14]  # changed
    assert [edges[1][4], edges[1][5], edges[1][6]] == [9, 740.83, 9762.14]  # new edge
    assert [edges[1][7], edges[1][8], edges[1][9]] == [14, 490.92, 14564.3]  # like previous edge
