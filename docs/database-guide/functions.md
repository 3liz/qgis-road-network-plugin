# Useful functions

The `road_graph` schema provides SQL functions which can be used directly in your queries,
for example in the QGIS **Browser** panel (right-click on the PostgreSQL connection or schema, then `Execute SQL…`),
in `psql` or in a **virtual layer** built on a PostgreSQL query.

The examples below use the coordinate reference system `EPSG:2154` and the road `D613`
of the test data of the plugin: the values can be slightly different in your database.

## Linear referencing functions

| Function | Usage |
|----------|-------|
| [get_reference_from_point](#get-the-references-of-a-point) | References of a point |
| [get_road_point_from_reference](#get-a-point-from-references) | Point corresponding to references |
| [get_road_substring_from_references](#get-a-line-between-two-references) | Line between two references |
| [get_spatial_road](#get-the-geometry-of-a-road-with-measures) | Geometry of a whole road, with the cumulative distances as measures |

### Get the references of a point

`road_graph.get_reference_from_point(_point, _road_code, _use_cache)` returns the **references** of a point,
as a JSON object:

```sql
SELECT road_graph.get_reference_from_point(
    ST_SetSRID(ST_MakePoint(473136.24, 6895774.58), 2154)
);
```

```json
{
    "road_code": "D613", "marker_code": 44, "abscissa": 154.50,
    "cumulative": 43063.50, "offset": 15.00, "side": "left"
}
```

* `_point`: the point, in the coordinate reference system of the road graph,
* `_road_code` (optional): the code of the road to use. If it is not given, the **closest road** is used.
  Give it to choose the road of a point located near an intersection,
* `_use_cache` (optional, `False` by default): used internally to speed up the calculation for many points of the same road.

The function returns `NULL` if no road is closer than **50 m** from the point.

Example: get the references of all the features of a point layer, without storing them:

```sql
SELECT
    t.id,
    r.ref->>'road_code' AS road_code,
    (r.ref->>'marker_code')::integer AS marker_code,
    (r.ref->>'abscissa')::real AS abscissa
FROM managed.demo_trees AS t,
LATERAL (SELECT road_graph.get_reference_from_point(t.geom, t.road_code) AS ref) AS r;
```

### Get a point from references

`road_graph.get_road_point_from_reference(_road_code, _marker_code, _abscissa, _offset, _side)` returns the **point**
corresponding to the given references, in the key `geom` of a JSON object (GeoJSON geometry):

```sql
SELECT road_graph.get_road_point_from_reference('D613', 44, 154.5, 15, 'left');
```

```json
{
    "road_code": "D613", "marker_code": 44, "abscissa": 154.5, "offset": 15, "side": "left",
    "closest_marker_abscissa": 0,
    "geom": {"type": "Point", "coordinates": [473136.24, 6895774.58], "crs": {...}}
}
```

* `_offset`: distance in meters from the road axis. Use `0` for a point on the road,
* `_side`: `left` or `right`, in the direction of the road.

To get a PostGIS geometry:

```sql
SELECT ST_GeomFromGeoJSON(
    road_graph.get_road_point_from_reference('D613', 44, 154.5, 15, 'left')->>'geom'
) AS geom;
```

### Get a line between two references

`road_graph.get_road_substring_from_references(_road_code, _start_marker_code, _start_marker_abscissa,
_end_marker_code, _end_marker_abscissa, _offset, _side, _from_first_edge_start, _to_last_edge_end)`
returns the **line** of the road between two references, in the key `geom` of a JSON object:

```sql
-- Line of the road D613 between the references 44 + 100 m and 44 + 600 m
SELECT ST_Length(
    (road_graph.get_road_substring_from_references('D613', 44, 100, 44, 600, 0, 'right')->>'geom')::geometry
);
-- 500.00
```

* the result is a **MultiLineString**: the road can be made of several parts, for example on both sides
  of a roundabout, which has its own road code,
* `_offset` and `_side`: to shift the line from the road axis, e.g. `3` and `left` for a painting on the left side,
* `_from_first_edge_start` and `_to_last_edge_end` (optional, `False` by default): extend the line
  to the start of the edge of the start references, and to the end of the edge of the end references,
* the start references must be **before** the end references along the road, else an error is raised.
  If the start marker is before the first marker of the road, the line starts at the start of the road,
  and if the end marker is after the last marker, the line ends at the end of the road.

### Get the geometry of a road with measures

`road_graph.get_spatial_road(_road_code)` returns the geometry of the **whole road**, as a MultiLineString
with **measures (M)**: the measure of each vertex is its cumulative distance from the start of the road.
It can be used with the PostGIS functions of linear referencing, for example `ST_LocateAlong`:

```sql
-- Point located 5000 m after the start of the road D613
SELECT ST_AsText(ST_LocateAlong(road_graph.get_spatial_road('D613'), 5000));
-- MULTIPOINT M ((507254.37 6896996.74 5000))
```

## Business data functions

These functions update the business tables listed in the `managed_objects` layer.
They are described in the page [Managed objects](../user-guide/managed-objects.md):

| Function | Usage |
|----------|-------|
| `update_table_references_from_geometries` | [Calculate the references of the features of a table from their geometries](../user-guide/managed-objects.md#calculate-the-references) |
| `update_table_geometries_from_references` | Calculate the geometries of the features of a table from their references (same page) |
| `add_managed_object_reference_fields` | [Add the reference fields to a table](../user-guide/managed-objects.md#add-the-reference-fields) |
| `add_managed_objects_triggers` | [Create the triggers calculating the references automatically](../user-guide/managed-objects.md#automatic-calculation-of-the-references) |

## GPS tracks: from a GPX file to the road references

A GPS track recorded on the road, for example with a smartphone and exported as a **GPX file**,
is never exactly on the road graph edges: the GPS precision is usually between **5 and 15 meters**.

Two functions allow to transform such a track into **road references**:

1. `road_graph.get_snapped_line_from_track` builds a line **snapped on the road graph edges**
   from the imprecise GPS track,
2. `road_graph.get_road_sections_from_snapped_line` returns the **ordered list of the roads** followed by this line,
   with the **start and end references** (marker and abscissa) for each road.

### Import the GPX file in PostgreSQL

A GPX file can be opened in QGIS with a drag & drop: choose the `tracks` layer.
Then import it in the PostgreSQL database with the QGIS **Browser** panel: drag & drop the layer
from the `Layers` panel onto the target schema of your PostgreSQL connection.
The Processing algorithm `Export to PostgreSQL` can also be used.

The import can also be done with `ogr2ogr`:

```bash
ogr2ogr -f PostgreSQL PG:"service=my_service" my_track.gpx tracks \
  -nln gps.tracks -lco GEOMETRY_NAME=geom -lco FID=id
```

!!! tip
    The tracks can stay in the GPS projection `EPSG:4326`: the functions reproject them automatically
    to the road graph projection. A geometry **without SRID** is considered to be already
    in the road graph projection.

### Snap the track on the road graph

```sql
SELECT road_graph.get_snapped_line_from_track(
    _track => t.geom,
    _densify_distance => 2,
    _max_distance => 25,
    _road_change_penalty => 100
) AS geom
FROM gps.tracks AS t
WHERE t.id = 1
;
```

Only the first parameter is required:

| Parameter              | Default | Description                                                                                  |
|------------------------|---------|----------------------------------------------------------------------------------------------|
| `_track`               |         | The GPS track: a linestring or a multilinestring, in any projection                          |
| `_densify_distance`    | `2`     | Distance in meters between the points added to the track before snapping them                |
| `_max_distance`        | `25`    | The track points further than this distance (meters) from any edge are ignored               |
| `_road_change_penalty` | `100`   | Penalty applied each time the road changes. Increase it if the result still has zigzags      |

The function returns a **multilinestring** in the road graph projection, which **follows exactly the edges geometries**,
in the **direction of the track**.

In the example below, the GPS track and its points are displayed in **cyan**, and the snapped line in **green**.
The track goes from the road `D40` to the road `D16` through the roundabout `GIR_D40_D16-1`:

![GPS track and snapped line](../media/database-guide/gps_track_snapping_overview.jpg)

Near the roundabout, the GPS track cuts the curves and crosses the roundabout,
whereas the snapped line follows the edges of the 3 roads:

![GPS track and snapped line near the roundabout](../media/database-guide/gps_track_snapping_zoom.jpg)

How does it work?

* The track is **densified** with a point every `_densify_distance` meters.
* For each point, all the roads closer than `_max_distance` are candidates.
* Simply taking the closest road for each point would produce **zigzags between two close roads**
  (parallel roads, intersections, roundabouts). Instead, the function searches the **sequence of roads**
  which minimizes, for the whole track:
    * the distance between the points and their road (multiplied by `_densify_distance`, which gives
      approximately the surface in m² between the track and the roads),
    * plus `_road_change_penalty` **each time the road changes** between two consecutive points.
* Each point is then snapped on the closest edge of its road. The small back and forth movements
  created near the intersections are removed.
* If two consecutive edges are not connected, for example when a short roundabout section has been skipped,
  the gap is filled with the **shortest path** of maximum 5 edges. If no path is found,
  a new part is created in the multilinestring.
* The result is built from the **edges geometries**, from node to node.

### Get the road sections references

```sql
SELECT s.*
FROM road_graph.get_road_sections_from_snapped_line(
    road_graph.get_snapped_line_from_track(
        ST_GeomFromText('LINESTRING(-0.044052 49.020203, -0.043663 49.019952, ...)', 4326)
    )
) AS s
;
```

The function returns **one line per road section**, ordered along the line:

| road_order | road_code     | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative | direction  | length |
|------------|---------------|--------------|----------------|------------------|------------|--------------|----------------|------------|--------|
| 1          | D40           | 14           | 905.27         | 16361.8          | 15         | 396.15       | 16761.4        | downstream | 399.6  |
| 2          | GIR_D40_D16-1 | 0            | 0              | 0                | 0          | 39.17        | 39.17          | downstream | 39.17  |
| 3          | D16           | 0            | 716.82         | 716.82           | 0          | 1.62         | 1.62           | upstream   | 715.19 |

* `start_marker` & `start_abscissa`: the references of the section start, e.g. `14+905.27`,
* `end_marker` & `end_abscissa`: the references of the section end,
* `start_cumulative` & `end_cumulative`: the distances from the road start,
* `direction`: `downstream` if the line follows the road direction, `upstream` otherwise,
* `length`: the section length in meters,
* `geom`: the section geometry (linestring).

The second parameter `_tolerance` (default `1` meter) is the maximum distance between the line and the edges.
The line must follow the road graph edges: use the result of `get_snapped_line_from_track`.

### Process all the tracks of a table

```sql
-- Snapped tracks
CREATE TABLE gps.snapped_tracks AS
SELECT
    t.id,
    road_graph.get_snapped_line_from_track(t.geom) AS geom
FROM gps.tracks AS t
;

-- Road sections of each track
CREATE TABLE gps.track_road_sections AS
SELECT
    st.id AS track_id,
    s.*
FROM gps.snapped_tracks AS st
CROSS JOIN LATERAL road_graph.get_road_sections_from_snapped_line(st.geom) AS s
ORDER BY st.id, s.road_order
;
```

### Limitations

* A **U-turn in the middle of an edge** is not kept: the line leaves the edge by the node it came from.
* With a very imprecise track (more than ~10 meters) in **complex areas** like highway interchanges,
  the result can contain wrong ramps, mainly at the track start or end.
  Increase `_road_change_penalty`, or cut the track ends.
* The points further than `_max_distance` from the road graph are ignored. If the track leaves the road graph
  for a long distance, the result can be split into several parts.
