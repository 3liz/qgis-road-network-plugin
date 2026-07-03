/**
 * @license Mozilla Public License Version 2.0
 * This script has been developed by the "community"
 * There isn't any guarantee that this script will work on another version of Lizmap Web Client.
 *
 */

var lizRoadNetwork = function () {

    // =================
    //  CONFIGURATION
    // =================
    ROAD_MIN_SCALE = 50000;
    ROAD_ROADS_LAYER_NAME = 'roads';
    ROAD_EDGES_LAYER_ID = 'edges_7df8a321_25e8_4a67_9bf8_796e4782e147';

    // =================
    // END CONFIGURATION
    // Do not edit below
    // =================

    // Global variables
    //
    // Hover related variables
    let hoverWait = false;
    let hoverPixel = null;

    // Global variable to indicate if we are waiting for the point geometry in the editing form
    ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM = false;

    // State of the references form in the edition form
    // We have a state variable to know it the references in the form has been filled
    // by the action get_references_from_point
    let ROAD_EDITION_FORM_REFERENCES_FILLED = {
        start: false,
        end: false
    };
    ROAD_EDITION_FORM_AUTO_GEOM = false;

    /**
     * Initialize the tool
     * @returns {boolean}
     */
    function init() {
        // console.log('lizRoadNetwork initialized');
        // Add interface
        addInterface();

        // Listen to Lizmap events
        lizMap.events.on({
            'minidockopened': function(evt) {
                // console.log(evt);
                if (evt.id == 'road_network') activateMapClickTool();
            },

            'minidockclosed': function(evt) {
                // console.log(evt);
                if (evt.id == 'road_network') {
                    deactivateMapClickTool();
                    lizMap.mainLizmap.action.resetLizmapAction();
                }
                if (evt.id = 'edition') {
                    // Reactivate the checkbox to allow activating the hover tool when the edition form is closed
                    const toggleHover = document.getElementById('rd_toggle_hover');
                    toggleHover.removeAttribute('disabled');
                }
            },

            'actionResultReceived': function(evt){
                if (evt.action.name == 'get_references_from_point') {
                    onGetReferencesFromPointReceived(evt);
                } else if (
                    evt.action.name == 'get_road_point_from_reference'
                ) {
                    onGetRoadGeometryFromReferenceReceived(evt);
                } else if (
                    evt.action.name == 'get_road_substring_from_references'
                ) {
                    onGetRoadGeometryFromReferenceReceived(evt);
                }
            },

            'lizmapeditionformdisplayed': function(event) {
                // Ajout du formulaire de création de géométrie à partir des références
                addBuildGeometryElements();

                // Deactivate the hover tool when the edition form is displayed
                const toggleHover = document.getElementById('rd_toggle_hover');
                if (toggleHover.checked) {
                    toggleHover.checked = false;
                    deactivateHoverTool();
                }

                // Deactivate the checkbox to avoid activating the hover tool when the edition form is displayed
                toggleHover.setAttribute('disabled', 'disabled');
            },

            'lizmapeditionfeaturecreated': function(event) {
                // Show the mini-dock if it is closed
                const roadMenu = document.querySelector('#mapmenu li.nav-minidock.road_network');
                if (!roadMenu.classList.contains('active')) {
                    roadMenu.querySelector('a').click();
                }

                // Reactivate the checkbox to allow activating the hover tool when the edition form is closed
                const toggleHover = document.getElementById('rd_toggle_hover');
                toggleHover.removeAttribute('disabled');
            },

            'lizmapeditionfeaturemodified': function(event) {
                // Show the mini-dock if it is closed
                const roadMenu = document.querySelector('#mapmenu li.nav-minidock.road_network');
                if (!roadMenu.classList.contains('active')) {
                    roadMenu.querySelector('a').click();
                }

                // Reactivate the checkbox to allow activating the hover tool when the edition form is closed
                const toggleHover = document.getElementById('rd_toggle_hover');
                toggleHover.removeAttribute('disabled');
            },

            'lizmapeditiongeometryupdated': function(event) {
                console.log(event);
                if (ROAD_EDITION_FORM_AUTO_GEOM == false) {
                    onEditingGeometryModified(event);
                }
            }
        });

        return true;
    }

    async function addInterface() {
        // Add mini-dock
        let html = `
        <form id="road_network_form" class="road_network_form">
        <table class="road_network_table" border="0" cellpadding="2" cellspacing="1">
            <tbody>
                <tr>
                    <th>Route</th>
                    <td colspan="2">
                        <input list="rd_road_code_list" id="rd_road_code" name="rd_road_code" value="D1" placeholder="Ex: D1"/>
                        <datalist id="rd_road_code_list" name="road_code_list">
                            <option value=""></option>
                        </datalist>
                    </td>
                </tr>
                <tr>
                    <th>PR</td>
                    <th>Abscisse</td>
                    <th>Cumul</td>
                </tr>
                <tr>
                    <td><input type="number" min="0" max="100" step="1"  id="rd_marker_code" name="marker_code" value="0" placeholder="Ex: 3"/></td>
                    <td><input type="number" min="0" max="2000" step="0.1"  id="rd_abscissa" name="abscissa" value="0" placeholder="Ex: 10.5"/></td>
                    <td><input type="number" min="0" max="500000" step="0.1"  id="rd_cumulative" name="cumulative" value="" placeholder="" disabled="disabled"/></td>
                </tr>
                <tr>
                    <th>Décalage</th>
                    <th>Côté</th>
                    <td></td>
                </tr>
                <tr>
                    <td><input type="number" min="0" max="100" step="0.1" id="rd_offset" name="offset" value="0" placeholder="Ex: 2.5"/></td>
                    <td>
                        <select id="rd_side" name="side">
                            <option value="0" selected>Gauche</option>
                            <option value="1">Droite</option>
                        </select>
                    </td>
                    <td>
                        <button id="rd_button_center_from_form_references" class="btn road_network_btn">Centrer</button>
                    </td>
                </tr>
                <tr>
                    <td colspan="3">
                        <div>
                            <input type="checkbox" id="rd_toggle_hover" name="toggle_hover">
                            <label for="rd_toggle_hover">Activer le calcul au survol</label>
                        </div>
                    </td>
                </tr>
            </tbody>
        </table>
        <input type="hidden" id="rd_point_wkt" name="point_wkt" />
        <input type="hidden" id="rd_fake_wkt" name="fake_wkt" />
        </form>
        `;
        lizMap.addDock(
            'road_network',
            'Graphe routier',
            'minidock',
            html,
            'icon-road'
        );

        // Fill the road combo box
        let data = await getWfsData(ROAD_ROADS_LAYER_NAME, 'id,road_code', null, 'none');
        if (data && data.features && data.features.length) {
            const roadCodeList = document.getElementById('rd_road_code_list');
            data.features.forEach(feature => {
                const option = document.createElement('option');
                option.value = feature.properties.road_code;
                option.dataset.id = feature.properties.id;
                roadCodeList.appendChild(option);
            })
        }

        // Show the minidock
        document.querySelector('#mapmenu li.road_network:not(.active) a').click();

        // Activate interface elements
        // ---

        // Toggle hover button
        const toggleHover = document.getElementById('rd_toggle_hover');
        toggleHover.addEventListener('change', evt => {
            onToggleHoverChange(evt)
        });

        // Get point from reference button:
        // will center the map on the point corresponding to the references filled in the form
        // We catch the submit event of the form instead of the click event of the button
        // but we will use the submit event to trigger the Lizmap action to get the road point from the references
        const form = document.getElementById('road_network_form');
        form.addEventListener('submit', evt => {
            evt.stopPropagation();
            evt.preventDefault();

            // Get the point from the references in the form and center the map on this point
            getRoadPointFromReferences('main');
        });

        // Activate the click on the map
        activateMapClickTool();
    }

    /**
     * Display road references when received from the Lizmap action
     * @param {Object} evt
     */
    function onGetReferencesFromPointReceived(evt) {
        // Get info box element
        const info = document.getElementById('rd_hover_info');

        // Display the result or a message
        if (info) {
            info.style.left = hoverPixel[0] + 'px';
            info.style.top = hoverPixel[1] + 'px';
            info.style.visibility = 'visible';
        }

        // If the Lizmap action returned a feature with road references
        if (
            evt
            && evt.features && evt.features.length == 1
            && evt.features[0].getProperties().road_code !== null
        ) {
            // Create references string
            const refs = evt.features[0].getProperties();
            const refsString = `${refs.road_code} PR ${refs.marker_code} + ${refs.abscissa}m (${refs.offset}m ${refs.side == 'left' ? 'G' : 'D'})`;

            // Display it on the map if info is active
            if (info) {
                info.innerText = refsString;
            }

            // Change the main form values if we are not in the context of the editing form
            const vertex_number = refs['vertex_number'];
            if (!lizMap.editionPending && vertex_number == -1) {
                const formInputs = ['road_code', 'marker_code', 'abscissa', 'cumulative', 'offset', 'side'];
                formInputs.forEach(inputName => {
                    const input = document.getElementById(`rd_${inputName}`);
                    if (!input) return;
                    input.value = refs[inputName];
                    if (inputName == 'abscissa' || inputName == 'cumulative' || inputName == 'offset') {
                        input.value = parseInt(refs[inputName]);
                    }
                    if (inputName == 'side') {
                        input.value = (refs[inputName] == 'left') ? 0 : 1;
                    }
                })
            }

            // Add the point WKT in a hidden input to be able to use it
            const wktFormat = new lizMap.ol.format.WKT();
            // clone feature to avoid modifying the original one
            const feature = evt.features[0];
            const clonedFeature = feature.clone();
            const clonedGeometry = clonedFeature.getGeometry();
            clonedGeometry.transform(lizMap.mainLizmap.projection, 'EPSG:4326');
            const wkt = wktFormat.writeGeometry(clonedGeometry);
            document.getElementById('rd_point_wkt').value = wkt;

            // Add the fake WKT in a hidden input to be able to use it
            const fakeWkt = createFakeWktFromForm('main');
            document.getElementById('rd_fake_wkt').value = fakeWkt;

            // If the editing form is opened, we the editing form with the references found at the clicked point
            if (lizMap.editionPending) {
                // Fields to fill in the editing form. We add a suffix "_end" for the end vertex if needed
                // We do not fill the offset & side fields to let the user decide
                const editingFormInputs = ['road_code', 'marker_code', 'abscissa'];
                editingFormInputs.forEach(inputName => {
                    const suffix = (vertex_number == 0) ? '' : '_end';
                    const inputId = `rd_editing_${inputName}${suffix}`;
                    const input = document.getElementById(inputId);
                    if (!input) {
                        console.log('* Input not found in editing form', inputId);
                        return;
                    }
                    console.log('* Filling input', inputId, 'with value', refs[inputName], 'in editing form');
                    if (inputName == 'abscissa') {
                        input.value = parseInt(refs[inputName]);
                    } else if (inputName == 'side') {
                        input.value = (refs[inputName] == 'left') ? 0 : 1;
                    } else {
                        input.value = refs[inputName];
                    }
                })

                // Set the global variable to indicate that the references in the editing form have been filled
                if (vertex_number == -1 || vertex_number == 0) {
                    ROAD_EDITION_FORM_REFERENCES_FILLED.start = true;
                } else if (vertex_number == 1) {
                    ROAD_EDITION_FORM_REFERENCES_FILLED.end = true;
                }
            }

        } else {
            // No data found
            if (info) {
                info.innerText = 'Aucune référence trouvée';
            }
        }
    }

    /**
     * Do some specific actions after receiving
     * the geometry point or linestring feature corresponding
     * from the get_road_point_from_reference action
     * @param {Event} evt
     */
    function onGetRoadGeometryFromReferenceReceived(evt) {
        if (
            evt
            && evt.features && evt.features.length == 1
            && evt.features[0].getGeometry()
        ) {
            const feature = evt.features[0];
            const clonedFeature = feature.clone();
            const clonedGeometry = clonedFeature.getGeometry();
            if (!clonedGeometry) {
                return;
            }
            const wktFormat = new lizMap.ol.format.WKT();
            // Clone geometry to avoid modifying the original one
            const featureWkt = wktFormat.writeGeometry(
                clonedGeometry.transform(lizMap.mainLizmap.projection, 'EPSG:4326')
            );

            // Zoom to the geometry only if we are not in editing context
            if (!lizMap.editionPending) {
                if (featureWkt.startsWith('POINT')) {
                    // Store the point WKT in a hidden input to be able to use it
                    document.getElementById('rd_point_wkt').value = featureWkt;

                    // Zoom on the geometry center
                    const center = feature.getGeometry().flatCoordinates;
                    lizMap.mainLizmap.map.getView().setCenter(center);
                } else {
                    // Zoom to the geometry extent for linestrings
                    const extent = feature.getGeometry().getExtent();
                    lizMap.mainLizmap.map.getView().fit(extent, {
                        size: lizMap.mainLizmap.map.getSize(),
                        maxZoom: 20
                    });
                }
            }

            // If we are in the context of the editing form, force OL maps synchronization
            if (lizMap.editionPending && lizMap.mainLizmap.newOlMap == false) {
                lizMap.mainLizmap.newOlMap = true;
                lizMap.mainLizmap.newOlMap = false;
            }

            // If we are waiting for the point geometry in the editing form,
            // it means that the user wants to create a point geometry from the references filled in the form.
            // So we set the geometry of the feature being edited with the received point geometry.
            if (ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM) {
                const replaceGeometry = replaceEditingFeatureGeometry(featureWkt);
                // Reset the global variable to indicate that we are no longer waiting for the point geometry
                ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM = false;
            }

            // Deactivate the global variable to avoid infinite loops
            // when the editing geometry is modified
            if (ROAD_EDITION_FORM_AUTO_GEOM) {
                ROAD_EDITION_FORM_AUTO_GEOM = false;
            }
        } else {
            console.log('No geometry found for these references');
            // Display a message to the user
            displayMessage(
                `Aucun point trouvée pour ces références.
                <br/>Causes possible : la route n'existe pas, le PR n'existe pas, l'abscisse est trop grande.`,
                'info',
                3000
            );
        }

        // Reset the global variable
        ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM = false;
    }


    /**
     * Handle toggle hover button click.
     *
     * This button behaves as a toggle: when activated, it allows the user
     * to hover over the map and get the road reference at the hovered point.
     * When deactivated, it stops this behavior.
     */
    function onToggleHoverChange(evt) {
        const checkbox = evt.currentTarget;
        if (checkbox.checked) {
            activateHoverTool();
            displayMessage('Survolez la carte pour obtenir les références routières', 'info', 5000);
        } else {
            deactivateHoverTool();
            displayMessage('Calcul des références au survol désactivé', 'info', 1000);
        }
    }


    /**
     * Display a message
     */
    function displayMessage(msg, type = 'info', duration=60000) {
        // Remove previous message
        let msgElt = document.getElementById('lizmap-road-message');
        if (msgElt) msgElt.remove();

        // Display new message
        lizMap.addMessage(msg, type, true, duration).attr('id','lizmap-road-message');
    }

    /**
     * Callback method to handle map pointer move events and get road references at the hovered point
     */
    function onPointerMove(evt) {
        // Do not send a request if the scale is too small
        if (lizMap.mainLizmap.state.map.scaleDenominator > ROAD_MIN_SCALE) {
            displayMessage(
                `Veuillez zoomer au moins au 1/${ROAD_MIN_SCALE} pour afficher les références`,
                'info',
                3000
            );
            return;
        }

        // Get info box element
        const info = document.getElementById('rd_hover_info');

        // Cancel if the user is dragging the mouse pointer
        if (evt.dragging) {
            if (info) {
                info.innerText = '';
                info.style.visibility = 'hidden';
            }
            return;
        }

        // If currently throttled, ignore the request
        if (hoverWait) return;

        // Do no send a request if checkbox is not checked
        const checkbox = document.getElementById('rd_toggle_hover');
        if (!checkbox.checked) return;

        // Store pixel and target in global variables
        hoverPixel = evt.pixel;

        // Get coordinates of the hovered point and send a request to get road references at this point
        getReferencesFromPixel(hoverPixel);

        // Ignore any future requests
        hoverWait = true;
        setTimeout(function (event) {
            hoverWait = false;
        }, 100);
    }

    /**
     * Activate the hover tool
     */
    function activateHoverTool() {
        // Add info box
        const info = document.getElementById('rd_hover_info');
        if (!info) {
            let infoBox = document.createElement('div');
            infoBox.id = 'rd_hover_info';
            document.getElementById('map-content').appendChild(infoBox);
        }

        // Add OL pointermove event
        lizMap.mainLizmap.map.on('pointermove', (evt) => onPointerMove(evt));

        // Remove hover info when the mouse leaves the map
        lizMap.mainLizmap.map.getViewport().addEventListener('pointerleave', (evt) => {
            const info = document.getElementById('rd_hover_info');
            if (info) {
                info.innerText = '';
                info.style.visibility = 'hidden';
            }
        });

    }

    /**
     * Deactivate the hover tool
     */
    function deactivateHoverTool() {
        const info = document.getElementById('rd_hover_info');
        if (info) {
            info.remove();
        }

        // Remove OL pointermove event
        lizMap.mainLizmap.map.un('pointermove', (evt) => onPointerMove(evt));
    }

    /**
     * Display the references in the form
     * when the user clicks on the map
     */
    function onPointerClick(evt) {
        // Do not send a request if the road mini-dock is not visible
        const roadMenu = document.querySelector('#mapmenu li.nav-minidock.road_network');
        if (!roadMenu.classList.contains('active')) {
            return true;
        }

        // Do not send a request if the scale is too small
        if (lizMap.mainLizmap.state.map.scaleDenominator > ROAD_MIN_SCALE) {
            displayMessage(
                `Veuillez zoomer au moins au 1/${ROAD_MIN_SCALE} pour afficher les références`,
                'info',
                3000
            );
            return true;
        }

        // console.log('onPointerClick', evt.pixel);
        getReferencesFromPixel(evt.pixel);
    }

    /**
     * Activate the click on the map
     */
    function activateMapClickTool() {
        // console.log('activateMapClickTool');
        lizMap.mainLizmap.map.on('click', (evt) => onPointerClick(evt));
    }

    /**
     * Deactivate the click on the map
     */
    function deactivateMapClickTool() {
        // console.log('deactivateMapClickTool');
        lizMap.mainLizmap.map.un('click', (evt) => onPointerClick(evt));
    }


    /**
     * Get the references for the given OpenLayer pixel coords
     *
     * This method uses the Lizmap action get_references_from_point
     * to request references from the database.
     *
     * The returned values are displayed depending on the context
     */
    function getReferencesFromPixel(pixel) {

        // Get coordinates of the hovered point and send a request to get road references at this point
        let coords = lizMap.mainLizmap.map.getCoordinateFromPixel(pixel);
        const transform = lizMap.ol.proj.getTransform(lizMap.mainLizmap.projection, 'EPSG:4326');
        // Clone object to avoid modifying the original coordinates
        let newCoords = coords.slice();
        newCoords = transform(newCoords);
        // We use -1 in the Z coordinate to differentiate from the actions run from the editing form with linestrings
        const wkt = `POINT(${newCoords[0]} ${newCoords[1]} -1)`
        document.getElementById('rd_point_wkt').value = wkt;

        lizMap.mainLizmap.action.runLizmapAction(
            'get_references_from_point',
            'layer',
            ROAD_EDGES_LAYER_ID,
            null,
            wkt
        );
    }

    /**
     * Get features via WFS with a filter
     *
     * @param {string} typename WFS layer typename
     * @param {string} expFilter Expression filter. Can be an empty string ''
     * @param {string} geometryFormat Format of the geometry to return: 'extent', 'geom', 'none'
     *
     * @return {Promise} Tableau d'objets de la couche.
     */
    async function getWfsData(typename, propertyName=null, expFilter=null, geometryFormat='none') {
        // Build URL and parameters
        const wfsUrl = ('wms' in lizUrls) ? lizUrls.wms : lizUrls.service;

        let wfsParams = {};
        for (let param in lizMap.mainLizmap.wfs._defaultGetFeatureParameters) {
            wfsParams[param] = lizMap.mainLizmap.wfs._defaultGetFeatureParameters[param];
        }
        wfsParams['TYPENAME'] = typename;
        if (expFilter !== null) wfsParams['EXP_FILTER'] = expFilter;
        if (propertyName !== null) wfsParams['PROPERTYNAME'] = propertyName;
        wfsParams['GEOMETRYNAME'] = geometryFormat;

        try {
            const response = await fetch(wfsUrl, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify(wfsParams)
            });

            if (response.status === 200) {
                // Get the JSON response
                const features = await response.json();

                return features;
            }
        } catch (error) {
            console.error("Error:", error);
        }

        return null;
    }

    /**
     * Create a fake WKT linestring from the given references
     * This is used to store all the references in a single WKT geometry that can be easily manipulated and sent to the server.
     *
     * @param {Object} refs An object containing the references: road_code, marker_code, abscissa, offset, side
     * @return {string} A WKT linestring like this: "LINESTRING (1 road_code_id, 2 marker_code, 3 abscissa, 4 offset, 5 side)"
     */
    function createFakeWktFromReferences(refs) {
        if (!refs.road_code || !refs.marker_code || !refs.abscissa) {
            return null;
        }
        const keys = ['road_code', 'marker_code', 'abscissa', 'offset', 'side'];
        let fakePoints = [];
        keys.forEach(key => {
            let wktValue = '';
            let inputValue = refs[key] ?? '0';
            if (key == 'road_code') {
                const road_code_id = document.querySelector(`#rd_road_code_list option[value=${inputValue}]`).dataset.id;
                wktValue = road_code_id;
            } else {
                wktValue = inputValue;
            }
            const type = keys.indexOf(key) + 1;
            fakePoints.push(`${type} ${wktValue}`);
        });
        if (fakePoints.length != 5) {
            return null;
        }

        return `LINESTRING(${fakePoints.join()})`;

    }

    /**
     * Create a fake WKT Linestring from the form values
     *
     * It returns a WKT like this: "LINESTRING (0 road_code ID, 1 marker_code, 2 abscissa, 3 offset, 4 side)"
     *
     * @param {string} sourceForm The source form: 'editing' or 'main'. Default is 'main'.
     * @returns {string|null}
     */
    function createFakeWktFromForm(sourceForm = 'main') {
        const inputs = ['road_code', 'marker_code', 'abscissa', 'offset', 'side'];
        const refs = {};
        const prefix = (sourceForm == 'editing') ? 'rd_editing_' : 'rd_';
        inputs.forEach(inputName => {
            const inputValue = document.getElementById(`${prefix}${inputName}`).value;
            // If the value is empty, add a default value of 0
            if (inputValue === '') {
                refs[inputName] = (inputName == 'road_code') ? 'D1' : '0';
                return;
            }

            refs[inputName] = inputValue;
        });
        const wkt = createFakeWktFromReferences(refs);

        return wkt;
    }

    /**
     * Get the point coordinates from the references in the form.
     *
     * Depending on the source form, center the map on this point
     * or set the geometry of the feature being edited with this point.
     *
     * @param {string} sourceForm The source form: 'editing' or 'main'. Default is 'main'.
     */
    function getRoadPointFromReferences(sourceForm = 'main') {
        // Create a fake WKT Linestring containing the form values as Y coordinates
        const fakeWkt = createFakeWktFromForm(sourceForm);
        if (!fakeWkt) {
            displayMessage(
                'Veuillez remplir tous les champs de référence pour centrer la carte',
                'info',
                2000
            );
            return;
        }
        // Store the fake WKT in a hidden input to be able to use it
        document.getElementById('rd_fake_wkt').value = fakeWkt;

        lizMap.mainLizmap.action.runLizmapAction(
            'get_road_point_from_reference',
            'layer',
            ROAD_EDGES_LAYER_ID,
            null,
            fakeWkt
        );
    }

    /**
     * Get the road substring linestring from the given two references
     */
    function getRoadSubstringFromReferences() {

        // Line geometry
        // Get start and end references from the form
        const startReferences = {
            road_code: document.getElementById('rd_editing_road_code').value,
            marker_code: document.getElementById('rd_editing_marker_code').value,
            abscissa: document.getElementById('rd_editing_abscissa').value,
            offset: document.getElementById('rd_editing_offset').value,
            side: document.getElementById('rd_editing_side').value,
        };
        const endReferences = {
            road_code: document.getElementById('rd_editing_road_code').value,
            marker_code: document.getElementById('rd_editing_marker_code_end').value,
            abscissa: document.getElementById('rd_editing_abscissa_end').value,
            offset: 0,
            side: '0',
        };

        // Do not continue if the references for end and start are not for the same road
        if (startReferences.road_code && endReferences.road_code && startReferences.road_code != endReferences.road_code) {
            displayMessage(
                'Les références doivent être sur la même route pour obtenir la ligne correspondante',
                'info',
                5000
            );
            return;
        }

        // Get the fake WKT for start and end references. If they are the same, do not continue
        const fakeWktA = createFakeWktFromReferences(startReferences);
        const fakeWktB = createFakeWktFromReferences(endReferences);
        if (!fakeWktA || !fakeWktB || (fakeWktA == fakeWktB)) {
            displayMessage(
                `Veuillez enregistrer des références valides pour obtenir la ligne correspondante
                <br>Les PR et abscisses de départ et d'arrivée doivent être renseignés et différents
                `,
                'info',
                5000
            );
            console.log('Invalid references for end & start point', startReferences, endReferences);

            return;
        }

        // Build a MULTILINESTRING WKT containing the two fake WKT as linestrings
        // Reorder linestrings if needed
        let fakeWkt = '';
        if (parseInt(startReferences.marker_code) < parseInt(endReferences.marker_code)) {
            fakeWkt = `MULTILINESTRING(${fakeWktA.replace('LINESTRING', '')}, ${fakeWktB.replace('LINESTRING', '')})`;
        } else if (parseInt(startReferences.marker_code) == parseInt(endReferences.marker_code)) {
            if (parseFloat(startReferences.abscissa) <= parseFloat(endReferences.abscissa)) {
                fakeWkt = `MULTILINESTRING(${fakeWktA.replace('LINESTRING', '')}, ${fakeWktB.replace('LINESTRING', '')})`;
            } else {
                fakeWkt = `MULTILINESTRING(${fakeWktB.replace('LINESTRING', '')}, ${fakeWktA.replace('LINESTRING', '')})`;
            }
        }  else {
            fakeWkt = `MULTILINESTRING(${fakeWktB.replace('LINESTRING', '')}, ${fakeWktA.replace('LINESTRING', '')})`;
        }
        console.log('fakeWkt', fakeWkt);

        // Run the Lizmap action to get the road substring between point A and point B
        lizMap.mainLizmap.action.runLizmapAction(
            'get_road_substring_from_references',
            'layer',
            ROAD_EDGES_LAYER_ID,
            null,
            fakeWkt
        );
    }

    /**
     * Set the editing form geometry from the references filled in the form.
     *
     * This will replace the existing geometry of the feature being edited, if any.
     *
     * @return {boolean} True if the geometry has been set, false otherwise
     */
    function setEditingGeometryFromReferences() {
        // We first check that the user is currently editing a feature
        if (!lizMap.editionPending) {
            return false;
        }

        // First check the geometry type to edit
        const layerId = document.getElementById('jforms_view_edition_liz_layerId').value;
        // const featureId = document.getElementById('jforms_view_edition_liz_featureId').value;
        const lizmapEditionLayer = lizMap.getLayerConfigById(layerId)[1];
        const geometryType = lizmapEditionLayer.geometryType;

        // If the geometry type is not point or line, we cannot create a geometry from the references
        if (!['point', 'line'].includes(geometryType)) {
            displayMessage(
                `Le type de géométrie de la couche n\'est pas pris en charge pour la création à partir des références.
                Seuls les points et lignes sont supportés.
                Type de géométrie actuel : ${geometryType}`,
                'error',
                5000
            );

            return false;
        }

        // For point geometry type, we use the Lizmap get_road_point_from_reference action
        // that will return the WKT of the point corresponding to the references filled in the form.
        // For line geometry type, we must get the WKT by using a dedicated Lizmap action
        // that will return the WKT of the line between start and end references.
        ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM = true;
        if (geometryType == 'point') {
            // Run the action to get the point WKT from the form references
            // We use a global variable so that the callback of the action
            // knows that we are waiting for the point geometry to set it in the editing form
            getRoadPointFromReferences('editing');
        } else {
            getRoadSubstringFromReferences();
        }
    }

    /**
     * Replace the editing feature geometry with the given WKT geometry
     *
     * @param {string} wkt The WKT geometry to set as the editing feature geometry
     * @return {boolean} True if the geometry has been set, false otherwise
     */
    function replaceEditingFeatureGeometry(wkt) {
        // Get OL 2 layer
        let editLayer = lizMap.map.getLayersByName('editLayer')[0];
        if (!editLayer) {
            return false;
        }

        // Manage edition control
        // We must deactivate the draw control for new features
        // And deactivate the modify control for existing features
        let editLayerControls = lizMap.map.getControlsBy('layer', editLayer);
        for(let c in editLayerControls) {
            let ctrl = editLayerControls[c];
            if (ctrl.active) {
                ctrl.deactivate();
            }
        }

        // Create feature from WKT
        var format = new OpenLayers.Format.WKT({
            externalProjection: 'EPSG:4326',
            internalProjection: editLayer.projection
        });
        let feat = format.read(wkt);
        if (!feat) {
            displayMessage(
                "Un problème inconnu est survenu lors de la création de la géométrie",
                'error',
                3000
            );
            return false;
        }

        // Set feature id
        feat.fid = $('#edition-form-container form').find('input[name="liz_featureId"]').val();

        // Add feature
        editLayer.destroyFeatures();
        editLayer.addFeatures([feat]);

        // Resynchronize OL2 and new OL maps
        lizMap.mainLizmap.newOlMap = true;
        lizMap.mainLizmap.newOlMap = false;

        return true;
    }


    /**
     * Add interface elements to the editing form
     * to create a geometry from the references filled in the form
     *
     */
    function addBuildGeometryElements() {
        // First check the geometry type to edit
        const layerId = document.getElementById('jforms_view_edition_liz_layerId').value;
        const featureId = document.getElementById('jforms_view_edition_liz_featureId').value;
        const lizmapEditionLayer = lizMap.getLayerConfigById(layerId)[1];
        const geometryType = lizmapEditionLayer.geometryType;

        // If the geometry type is not point or line, we cannot create a geometry from the references
        if (!['point', 'line'].includes(geometryType)) {
            return;
        }

        // Create the HTML text to add above the editing form
        let html = `
        <details class="road_network_details">
        <summary>Créer la géométrie depuis des références</summary>
        <div>
        <form id="road_network_editing_form" class="road_network_form">
        <table class="road_network_table" border="0" cellpadding="2" cellspacing="1">
            <tbody>
                <tr>
                    <th>Route</th>
                    <td>
                        <!-- We use the same datalist as the main form to avoid duplicating the list of roads -->
                        <input list="rd_road_code_list" id="rd_editing_road_code" name="rd_road_code" value="D1" placeholder="Ex: D1"/>
                    </td>
                </tr>
                <tr>
                    <!-- For point, only PR and abscissa are needed. For line, we need PR and abscissa for start and end points. -->
                    <th>PR ${(geometryType == 'line') ? 'début' : ''}</td>
                    <th>Abscisse ${(geometryType == 'line') ? 'début' : ''}</td>
                </tr>
                <tr>
                    <td><input type="number" min="0" max="100" step="1"  id="rd_editing_marker_code" name="marker_code" value="0" placeholder="Ex: 3"/></td>
                    <td><input type="number" min="0" max="2000" step="0.1"  id="rd_editing_abscissa" name="abscissa" value="0" placeholder="Ex: 10.5"/></td>
                </tr>
        `;

        // Add end inputs for end PR and end abscissa for line geometry type
        if (geometryType == 'line') {
            html += `
                <tr>
                    <th>PR fin</td>
                    <th>Abscisse fin</td>
                </tr>
                <tr>
                    <td><input type="number" min="0" max="100" step="1"  id="rd_editing_marker_code_end" name="marker_code_end" value="0" placeholder="Ex: 5"/></td>
                    <td><input type="number" min="0" max="2000" step="0.1"  id="rd_editing_abscissa_end" name="abscissa_end" value="0" placeholder="Ex: 60"/></td>
                </tr>
            `;
        }

        // Add offset and side inputs
        html += `
                <tr>
                    <th>Décalage</th>
                    <th>Côté</th>
                </tr>
                <tr>
                    <td><input type="number" min="0" max="100" step="0.1" id="rd_editing_offset" name="offset" value="0" placeholder="Ex: 2.5"/></td>
                    <td>
                        <select id="rd_editing_side" name="side">
                            <option value="0" selected>Gauche</option>
                            <option value="1">Droite</option>
                        </select>
                    </td>
                    <td>
                        <button
                            id="rd_editing_build_geometry_from_references" class="btn road_network_btn"
                            title="Créer la géométrie à partir des références renseignées dans le formulaire.">Créer</button>
                    </td>
                </tr>
            </tbody>
        </table>
        </form>
        </div>
        </details>
        `;

        // Add the HTML above the editing form
        const editingForm = document.querySelector('form#jforms_view_edition');
        editingForm.insertAdjacentHTML('beforebegin', html);

        // We catch the references form submit event instead of the click event of the button
        const form = document.getElementById('road_network_editing_form');
        form.addEventListener('submit', evt => {
            evt.stopPropagation();
            evt.preventDefault();

            // Hide the mini-dock if it is open
            const roadMenu = document.querySelector('#mapmenu li.nav-minidock.road_network');
            if (roadMenu.classList.contains('active')) {
                roadMenu.querySelector('a').click();
            }

            // Get the geometry from the form references
            // depending on the type of the feature being edited (point or line),
            // we need different references
            let setGeometry = setEditingGeometryFromReferences();
            if (setGeometry) {
                // Display a message to the user
                displayMessage(
                    'Géométrie créée à partir des références.',
                    'info',
                    3000
                );
            }
        });

    }

    /**
     * Get references when the user modifies the geometry of the feature being edited.
     *
     * @param {Object} event The event object from the OpenLayers modify control
     * @returns
     */
    function onEditingGeometryModified(event) {
        ROAD_EDITION_FORM_REFERENCES_FILLED.start = false;
        ROAD_EDITION_FORM_REFERENCES_FILLED.end = false;
        if (!lizMap.editionPending) {
            return;
        }

        // Get the edition layer config from the lizMap config
        const lizmapEditionLayer = lizMap.getLayerConfigById(event.layerId)[1];
        const editionLayerConfig = lizMap.config.editionLayers[lizmapEditionLayer.name];
        var geometryType = editionLayerConfig.geometryType; // point or line

        // Get the geometry of the feature being edited
        const geom = event.geometry;
        clonedGeom = geom.clone();
        clonedGeom.transform("EPSG:"+event.srid, 'EPSG:4326');

        console.log('Feature modified, geometry type:', geometryType, 'geometry:', clonedGeom);

        // Depending on the geometry type, we get the coordinates of the point or the first and last vertex of the line
        if (geometryType == 'point') {
            // Get the point coordinates
            const wkt = `POINT(${clonedGeom.x} ${clonedGeom.y} 0)`;
            console.log('Feature modified, point geometry WKT:', wkt);
            lizMap.mainLizmap.action.runLizmapAction(
                'get_references_from_point',
                'layer',
                ROAD_EDGES_LAYER_ID,
                null,
                wkt
            );

        } else if (geometryType == 'line') {
            // It depends on the type : MULTI or simple
            let firstWkt = '';
            let lastWkt = '';
            if (clonedGeom.CLASS_NAME == 'OpenLayers.Geometry.MultiLineString') {
                console.log('Feature modified, line geometry is a MultiLineString with', clonedGeom.components.length, 'components');
                // Get the line first and last vertex coordinates
                // We use the third coordinate to indicate the order of the vertex: 0 for first, 1 for last
                const firstVertex = clonedGeom.components[0].components[0];
                const lastVertex = clonedGeom.components[clonedGeom.components.length - 1].components[clonedGeom.components[clonedGeom.components.length - 1].components.length - 1];
                firstWkt = `POINT(${firstVertex.x} ${firstVertex.y} 0)`;
                lastWkt = `POINT(${lastVertex.x} ${lastVertex.y} 1)`;
            } else {
                console.log('Feature modified, line geometry is a LineString with', clonedGeom.components.length, 'components');
                // Get the line first and last vertex coordinates
                // We use the third coordinate to indicate the order of the vertex: 0 for first, 1 for last
                const firstVertex = clonedGeom.components[0];
                const lastVertex = clonedGeom.components[clonedGeom.components.length - 1];
                firstWkt = `POINT(${firstVertex.x} ${firstVertex.y} 0)`;
                lastWkt = `POINT(${lastVertex.x} ${lastVertex.y} 1)`;
            }

            if (!firstWkt || !lastWkt) {
                console.log('Feature modified, line geometry is invalid, cannot get first and last vertex WKT');
                return;
            }
            console.log('Feature modified, line first vertex WKT:', firstWkt);
            lizMap.mainLizmap.action.runLizmapAction(
                'get_references_from_point',
                'layer',
                ROAD_EDGES_LAYER_ID,
                null,
                firstWkt
            );
            console.log('Feature modified, line last vertex WKT:', lastWkt);
            lizMap.mainLizmap.action.runLizmapAction(
                'get_references_from_point',
                'layer',
                ROAD_EDGES_LAYER_ID,
                null,
                lastWkt
            );
        }

        // Create the geometry once the references have been filled in the form.
        // We wait for the references to be filled in the form before getting the geometry,
        // because the get_references_from_point action is asynchronous
        // and we cannot guarantee that the references will be filled in the form before we get the geometry.
        // We do it only if the linestring
        const refForm = document.getElementById('road_network_editing_form');
        if (!refForm) {
            console.log('References form not found, cannot get the geometry from the references');
            return;
        }

        // Recreate the geometry every time it has been modified, but only if the references have been filled in the form.
        // Only loop 10 times because the loop will not end if the user modifies the geometry with a node outside the 50m
        const maxAttempts = 10;
        let attempts = 0;
        const intervalId = setInterval(() => {
            // Abort if we reach the maximum number of attempts
            if (attempts >= maxAttempts) {
                clearInterval(intervalId);
                console.log('Max attempts reached, aborting');

                // Display a message to the user
                displayMessage(
                    `La géométrie n'a pas pu être créée automatiquement à partir des références.
                    <br/>Cause possible : le point de la géométrie est trop éloigné de la route (plus de 50m).`,
                    'info',
                    5000
                );
                return;
            }

            // Set the geometry from the references only if the references have been filled in the form
            if (!ROAD_EDITION_FORM_AUTO_GEOM
                && (
                    (geometryType == 'point' && ROAD_EDITION_FORM_REFERENCES_FILLED.start)
                    ||
                    (geometryType == 'line' && ROAD_EDITION_FORM_REFERENCES_FILLED.start && ROAD_EDITION_FORM_REFERENCES_FILLED.end)
                )
            ) {
                clearInterval(intervalId);
                console.log('References filled in the form, now we can get the geometry from the references');
                ROAD_EDITION_FORM_AUTO_GEOM = true;
                setEditingGeometryFromReferences();
            } else {
                console.log('Waiting for references to be filled in the form before getting the geometry');
                console.log('max Attempts left:', maxAttempts-attempts);
            }
            attempts++;
        }, 200);

    }

    /**
     * Wait for lizMap and mainLizmap objects to be available
     * @returns {Promise<boolean>}
     */
    function waitForObject() {
        return new Promise((resolve, reject) => {
            const intervalId = setInterval(() => {
                if (lizMap && lizMap.mainLizmap && lizMap.mainLizmap.action) {
                    clearInterval(intervalId);
                    resolve(true);
                }
            }, 200); // Check every 200 milliseconds for availability of listed objects
        });
    }

    // Start when lizMap and mainLizmap are available
    waitForObject().then((response) => {
        init();
    });

    // Public API
    let obj = {
        data: {
            ROAD_EDITION_FORM_AUTO_GEOM
        },
    }

    return obj;

}();


/*
TODO

*/
