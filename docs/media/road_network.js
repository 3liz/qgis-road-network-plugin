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
    const ROAD_MIN_SCALE = 50000;
    const ROAD_ROADS_LAYER_NAME = 'roads';
    const ROAD_EDGES_LAYER_ID = 'edges_7df8a321_25e8_4a67_9bf8_796e4782e147';
    const ROAD_ORDER_FIELD = 'road_order';
    const ROAD_MERGE_LINES_STATE = {
        'layer_id': null,
        'feature_id_a': null,
        'feature_id_b': null
    };
    const ROAD_MERGE_LINES_BUTTONS = {
        0: {
            'text': 'Choisir cet objet pour fusion',
            'title': 'Choisir cet objet pour pouvoir ensuite le fusionner avec un autre objet de la même couche'
        },
        1: {
            'text': 'Fusionner avec l\'objet choisi',
            'title': 'En cliquant sur ce bouton, vous pouvez lancer la fusion entre cet objet et l\'objet précédemment choisi.'
        },
        2: {
            'text': 'Fusion en cours...',
            'title': 'La fusion des deux lignes est en cours. Veuillez patienter'
        },
        3: {
            'text': 'Objet choisi pour la fusion',
            'title': 'Cet objet a été choisi pour être fusionné avec un autre objet. Veuillez cliquer sur le bouton de fusion d\'un autre objet, ou recliquer ici pour annuler'
        }
    };

    // =================
    // END CONFIGURATION
    // Do not edit below
    // =================

    // Hover related variables
    let HOVER_WAIT = false;
    let HOVER_WAIT_MILLISECONDS = 200;
    let HOVER_PIXEL = null;

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
                if (evt.id == 'edition') {
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
                    evt.action.name == 'get_edge_start_or_end_tip_from_references'
                ) {
                    onGetEdgeStartOrEndTipFromReferencesReceived(evt);
                } else if (
                    evt.action.name == 'get_road_substring_from_references'
                ) {
                    onGetRoadGeometryFromReferenceReceived(evt);
                } else if (
                    evt.action.name == 'split_line_by_point'
                ) {
                    // Clear the hightlighted features
                    lizMap.mainLizmap.map.clearHighlightFeatures();
                } else if (
                    evt.action.name == 'merge_selected_lines'
                ) {
                    // Clear the hightlighted features
                    lizMap.mainLizmap.map.clearHighlightFeatures();

                    // Get object layer and feature ids
                    const featureId = ROAD_MERGE_LINES_STATE.feature_id_b;
                    const layerId = ROAD_MERGE_LINES_STATE.layer_id;

                    // Clean the popup : keep only the merged feature (to avoid issues)
                    document.querySelectorAll(`div.lizmapPopupSingleFeature[data-layer-id="${layerId}"]:not([data-feature-id="${featureId}"])`).forEach(elt => {
                        elt.remove();
                    });

                    // Refresh the visible button to its initial state
                    const button = document.querySelectorAll(`button.road_network_btn[data-action-name="${evt.action.name}"]`).forEach(bt => {
                        bt.innerText = ROAD_MERGE_LINES_BUTTONS[0].text;
                        bt.title = ROAD_MERGE_LINES_BUTTONS[0].title;
                        bt.disabled = false;
                        bt.classList.toggle('active', false);
                    });

                    // Clear merge line global object
                    ROAD_MERGE_LINES_STATE.layer_id = null;
                    ROAD_MERGE_LINES_STATE.feature_id_a = null;
                    ROAD_MERGE_LINES_STATE.feature_id_b = null;

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
                // console.log(event);
                if (ROAD_EDITION_FORM_AUTO_GEOM == false) {
                    onEditingGeometryModified(event);
                }
            },

            'lizmappopupdisplayed': function(popup, containerId) {
                // Add button to allow the user the split the chosen line at the clicked point and create two new lines
                // Get popup container
                let popupContainerId = popup.containerId;
                let popupContainer = document.getElementById(popupContainerId);
                if (!popupContainer) {
                    return true;
                }
                let featureIdInputSelector = 'div.lizmapPopupContent input.lizmap-popup-layer-feature-id';
                // For each popup, add some tools depending on the status of the authenticated user
                Array.from(popupContainer.querySelectorAll(featureIdInputSelector)).map(element => {

                    // Get layer id and feature id
                    let val = element.value;
                    let featureId = val.split('.').pop();
                    let layerId = val.replace('.' + featureId, '');

                    // Add popup buttons
                    addSplitPopupButton(element, layerId, featureId);
                    addMergePopupButton(element, layerId, featureId);
                });

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
                            <option value="left" selected>Gauche</option>
                            <option value="right">Droite</option>
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
        // The field "ordre" is based on the road_code length, which will help
        // to show the big road first (with a simple and smaller code)
        let data = await getWfsData(ROAD_ROADS_LAYER_NAME, 'id,road_code', null, 'none', ROAD_ORDER_FIELD);
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
            info.style.left = HOVER_PIXEL[0] + 'px';
            info.style.top = HOVER_PIXEL[1] + 'px';
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
            const refsString = `${refs.road_code} - PR ${refs.marker_code} + ${parseInt(refs.abscissa)}m (${refs.offset}m ${refs.side == 'left' ? 'G' : 'D'})`;

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
                        input.value = (refs[inputName] == 'left') ? 'left' : 'right';
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

            // If the editing form is opened, we fill the editing form with the references found at the clicked point
            if (lizMap.editionPending) {
                // Fields to fill in the editing form. We add a suffix "_end" for the end vertex if needed
                // We do not fill the offset & side fields to let the user decide
                const editingFormInputs = ['road_code', 'marker_code', 'abscissa'];
                editingFormInputs.forEach(inputName => {
                    const suffix = (vertex_number == 0) ? '' : '_end';
                    const inputId = `rd_editing_${inputName}${suffix}`;
                    const input = document.getElementById(inputId);
                    if (!input) {
                        // console.log('* Input not found in editing form', inputId);
                        return;
                    }
                    // console.log('* Filling input', inputId, 'with value', refs[inputName], 'in editing form');
                    if (inputName == 'abscissa') {
                        input.value = parseInt(refs[inputName]);
                    } else if (inputName == 'side') {
                        input.value = (refs[inputName] == 'left') ? 'left' : 'right';
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
     * Update the start or end references written in the editing form
     * when receiving the references of the closest edge start or end point from the Lizmap action
     * @param {Event} evt
     * @returns
     */
    function onGetEdgeStartOrEndTipFromReferencesReceived(evt) {
        if (
            evt
            && evt.features && evt.features.length == 1
            && evt.features[0].getGeometry()
        ) {
            const feature = evt.features[0];
            const clonedFeature = feature.clone();
            const start_or_end = feature.getProperties().start_or_end;
            const marker_code = feature.getProperties().marker_code;
            const abscissa = feature.getProperties().abscissa;
            // console.log('onGetEdgeStartOrEndTipFromReferencesReceived', start_or_end, marker_code, abscissa);

            // Edit the editing form inputs with the received references
            const suffix = (start_or_end == 'start') ? '' : '_end';
            const markerInput = document.getElementById(`rd_editing_marker_code${suffix}`);
            const abscissaInput = document.getElementById(`rd_editing_abscissa${suffix}`);
            if (markerInput) {
                markerInput.value = marker_code;
            }
            if (abscissaInput) {
                abscissaInput.value = abscissa;
            }

            // Trigger the Lizmap action to get the road substring between the start and end references
            ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM = true;
            getRoadSubstringFromReferences();
        } else {
            // Display a message to the user
            displayMessage(
                `Pas d'extrémité de tronçon trouvée pour ces références. Calcul des nouvelles références annulé.`,
                'info',
                3000
            );
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
            // it means that the user wants to create a point geometry
            // from the references filled in the form.
            // So we set the geometry of the feature being edited with the received point geometry.
            if (ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM) {
                const replaceGeometry = replaceEditingFeatureGeometry(featureWkt);
                // Reset the global variable to indicate that we are no longer waiting for the geometry
                ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM = false;
            }

            // Deactivate the global variable to avoid infinite loops
            // when the editing geometry is modified
            if (ROAD_EDITION_FORM_AUTO_GEOM) {
                ROAD_EDITION_FORM_AUTO_GEOM = false;
            }
        } else {
            // console.log('No geometry found for these references');
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
        if (HOVER_WAIT) return;

        // Do no send a request if checkbox is not checked
        const checkbox = document.getElementById('rd_toggle_hover');
        if (!checkbox.checked) return;

        // Store pixel and target in global variables
        HOVER_PIXEL = evt.pixel;

        // Get coordinates of the hovered point and send a request to get road references at this point
        getReferencesFromPixel(HOVER_PIXEL);

        // Ignore any future requests
        HOVER_WAIT = true;
        setTimeout(function (event) {
            HOVER_WAIT = false;
        }, HOVER_WAIT_MILLISECONDS);
    }

    /**
     * Callback method to hide the hover info when the mouse leaves the map
     */
    function onPointerLeave() {
        const info = document.getElementById('rd_hover_info');
        if (info) {
            info.innerText = '';
            info.style.visibility = 'hidden';
        }
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
        // We pass the function itself (not an anonymous one)
        // so that the same reference can be used to remove the listener
        lizMap.mainLizmap.map.on('pointermove', onPointerMove);

        // Remove hover info when the mouse leaves the map
        lizMap.mainLizmap.map.getViewport().addEventListener('pointerleave', onPointerLeave);

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
        lizMap.mainLizmap.map.un('pointermove', onPointerMove);

        // Remove the pointerleave event
        lizMap.mainLizmap.map.getViewport().removeEventListener('pointerleave', onPointerLeave);
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
        // We pass the function itself (not an anonymous one)
        // so that the same reference can be used to remove the listener
        lizMap.mainLizmap.map.on('click', onPointerClick);
    }

    /**
     * Deactivate the click on the map
     */
    function deactivateMapClickTool() {
        // console.log('deactivateMapClickTool');
        lizMap.mainLizmap.map.un('click', onPointerClick);
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
        const wkt = `POINT(${newCoords[0]} ${newCoords[1]})`
        document.getElementById('rd_point_wkt').value = wkt;

        lizMap.mainLizmap.action.runLizmapAction(
            'get_references_from_point',
            'layer',
            ROAD_EDGES_LAYER_ID,
            null,
            wkt,
            {"vertex_number": -1, "road_code": ""}
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
    async function getWfsData(typename, propertyName=null, expFilter=null, geometryFormat='none', sortBy=null) {
        // Build URL and parameters
        const wfsUrl = ('wms' in lizUrls) ? lizUrls.wms : lizUrls.service;

        let wfsParams = {};
        for (let param in lizMap.mainLizmap.wfs._defaultGetFeatureParameters) {
            wfsParams[param] = lizMap.mainLizmap.wfs._defaultGetFeatureParameters[param];
        }
        wfsParams['TYPENAME'] = typename;
        if (expFilter !== null) wfsParams['EXP_FILTER'] = expFilter;
        if (propertyName !== null) wfsParams['PROPERTYNAME'] = propertyName;
        if (sortBy !== null) wfsParams['SORTBY'] = sortBy;
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
     * Create a object containing the needed parameters to send to the Lizmap action
     * from the given references
     *
     * @param {Object} refs An object containing the references: road_code, marker_code, abscissa, offset, side
     * @return {Object} An object with the needed parameters
     */
    function buildActionParametersFromReferences(refs) {
        if (!refs.road_code || !refs.marker_code || !refs.abscissa) {
            return null;
        }
        const keys = ['road_code', 'marker_code', 'abscissa', 'offset', 'side'];
        const expectedLength = keys.length;
        let refItems = [];
        let params = {};
        keys.forEach(key => {
            let paramValue = '';
            let inputValue = refs[key] ?? '0';
            if (key == 'road_code') {
                const road_code_id = document.querySelector(`#rd_road_code_list option[value=${inputValue}]`).dataset.id;
                paramValue = road_code_id;
            } else {
                paramValue = inputValue;
            }
            refItems.push(`${paramValue}`);
            params[key] = paramValue;
        });
        if (refItems.length != expectedLength) {
            return null;
        }

        return params;
    }

    /**
     * Create the objects containing the parametres to send to the Lizmap action
     * from the given form values
     *
     * It returns an object, or null if one of the required fields
     * (road_code, marker_code, abscissa) is empty.
     * Empty offset and side fields are replaced by their default values.
     *
     * @param {string} sourceForm The source form: 'editing' or 'main'. Default is 'main'.
     * @returns {Object|null}
     */
    function buildActionParametersFromForm(sourceForm = 'main') {
        const inputs = ['road_code', 'marker_code', 'abscissa', 'offset', 'side'];
        const requiredInputs = ['road_code', 'marker_code', 'abscissa'];
        // Default values for the optional fields
        let refs = {
            road_code: '',
            marker_code: '',
            abscissa: '',
            offset: '0',
            side: 'left'
        };
        const prefix = (sourceForm == 'editing') ? 'rd_editing_' : 'rd_';
        let missingRequired = false;
        inputs.forEach(inputName => {
            const inputElement = document.getElementById(`${prefix}${inputName}`);
            const inputValue = inputElement ? inputElement.value.trim() : '';
            if (inputValue === '') {
                if (requiredInputs.includes(inputName)) {
                    missingRequired = true;
                }
                return;
            }
            refs[inputName] = inputValue;
        });
        if (missingRequired) {
            return null;
        }

        return refs
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
        const actionParams = buildActionParametersFromForm(sourceForm);
        if (!actionParams) {
            const goal = (sourceForm == 'editing') ? 'créer la géométrie' : 'centrer la carte';
            displayMessage(
                `Veuillez renseigner la route, le PR et l'abscisse pour ${goal}`,
                'info',
                2000
            );
            // No action will be run, so we are no longer waiting for the geometry
            ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM = false;
            return;
        }

        lizMap.mainLizmap.action.runLizmapAction(
            'get_road_point_from_reference',
            'layer',
            ROAD_EDGES_LAYER_ID,
            null,
            null,
            actionParams
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
            side: 'left',
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
        const paramsA = buildActionParametersFromReferences(startReferences);
        const paramsB = buildActionParametersFromReferences(endReferences);
        // Compare the values, not the objects. Offset and side are not compared
        // because they are not used for the end references
        const sameReferences = (
            paramsA && paramsB
            && paramsA.road_code == paramsB.road_code
            && parseInt(paramsA.marker_code) == parseInt(paramsB.marker_code)
            && parseFloat(paramsA.abscissa) == parseFloat(paramsB.abscissa)
        );
        if (!paramsA || !paramsB || sameReferences) {
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

        // Build the action parameters
        // Invert start and edge values if needed
        let actionParams = {
            road_code: startReferences.road_code,
            start_marker_code: startReferences.marker_code,
            start_abscissa: startReferences.abscissa,
            end_marker_code: endReferences.marker_code,
            end_abscissa: endReferences.abscissa,
            offset: startReferences.offset,
            side: startReferences.side
        };
        let mustRevertStartAndEnd = false;
        if (parseInt(startReferences.marker_code) < parseInt(endReferences.marker_code)) {
            // Do nothing, the order is correct
            mustRevertStartAndEnd = false;
        } else if (parseInt(startReferences.marker_code) == parseInt(endReferences.marker_code)) {
            if (parseFloat(startReferences.abscissa) <= parseFloat(endReferences.abscissa)) {
                // Do nothing, the order is correct
                mustRevertStartAndEnd = false;
            } else {
                mustRevertStartAndEnd = true;
            }
        }  else {
            mustRevertStartAndEnd = true;
        }
        if (mustRevertStartAndEnd) {
            actionParams = {
                road_code: startReferences.road_code,
                start_marker_code: endReferences.marker_code,
                start_abscissa: endReferences.abscissa,
                end_marker_code: startReferences.marker_code,
                end_abscissa: startReferences.abscissa,
                offset: startReferences.offset,
                side: startReferences.side
            };
        }

        // Run the Lizmap action to get the road substring between point A and point B
        lizMap.mainLizmap.action.runLizmapAction(
            'get_road_substring_from_references',
            'layer',
            ROAD_EDGES_LAYER_ID,
            null,
            null,
            actionParams
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
                    <td colspan="2">
                        <!-- We use the same datalist as the main form to avoid duplicating the list of roads -->
                        <input list="rd_road_code_list" id="rd_editing_road_code" name="rd_road_code" value="" placeholder="Ex: D1"/>
                    </td>
                </tr>
                <tr>
                    <!-- For point, only PR and abscissa are needed. For line, we need PR and abscissa for start and end points. -->
                    <th>PR ${(geometryType == 'line') ? 'début' : ''}</td>
                    <th>Abscisse ${(geometryType == 'line') ? 'début' : ''}</td>
                    ${(geometryType == 'line') ? `<th title="Aller au début du tronçon ou de la route situés sous la position correspondante aux références">Début</th>` : '<th></th>'}
                </tr>
                <tr>
                    <td><input type="number" min="0" max="100" step="1"  id="rd_editing_marker_code" name="marker_code" value="0" placeholder="Ex: 3"/></td>
                    <td><input type="number" min="0" max="2000" step="0.1"  id="rd_editing_abscissa" name="abscissa" value="0" placeholder="Ex: 10.5"/></td>
                    <td style="vertical-align:center;">
                    ${(geometryType == 'line') ? `
                        <span class="road_network_go_to_tip edge_tip btn btn-sm"
                            id="rd_editing_go_to_start_edge"
                            title="Aller au début du tronçon situé sous la position correspondante aux références"
                        >Tronçon</span>
                        <span class="road_network_go_to_tip route_tip btn btn-sm"
                            id="rd_editing_go_to_start_route"
                            title="Aller au début de la route correspondante aux références"
                        >Route</span>
                        ` : ''}
                    </td>
                </tr>
        `;

        // Add end inputs for end PR and end abscissa for line geometry type
        if (geometryType == 'line') {
            html += `
                <tr>
                    <th>PR fin</td>
                    <th>Abscisse fin</td>
                    <th title="Aller à la fin du tronçon ou de la route situés sous la position correspondante aux références">Fin</th>
                </tr>
                <tr>
                    <td><input type="number" min="0" max="100" step="1"  id="rd_editing_marker_code_end" name="marker_code_end" value="0" placeholder="Ex: 5"/></td>
                    <td><input type="number" min="0" max="2000" step="0.1"  id="rd_editing_abscissa_end" name="abscissa_end" value="0" placeholder="Ex: 60"/></td>
                    <td style="vertical-align:center;">
                        <span class="road_network_go_to_tip edge_tip btn btn-sm"
                            id="rd_editing_go_to_end_edge"
                            title="Aller à la fin du tronçon situé sous la position correspondante aux références"
                        >Tronçon</span>
                        <span class="road_network_go_to_tip route_tip btn btn-sm"
                            id="rd_editing_go_to_end_route"
                            title="Aller à la fin de la route correspondante aux références"
                        >Route</span>
                    </td>
                </tr>
            `;
        }

        // Add offset and side inputs
        html += `
                <tr>
                    <th>Décalage</th>
                    <th>Côté</th>
                    <th></th>
                </tr>
                <tr>
                    <td><input type="number" min="0" max="100" step="0.1" id="rd_editing_offset" name="offset" value="0" placeholder="Ex: 2.5"/></td>
                    <td>
                        <select id="rd_editing_side" name="side">
                            <option value="left" selected>Gauche</option>
                            <option value="right">Droite</option>
                        </select>
                    </td>
                    <td>
                    </td>
                </tr>
                <tr>
                    <td>
                        <button
                            id="rd_editing_reset_geometry" class="btn road_network_btn"
                            title="Réinitialiser le dessin de la géométrie">Réinitialiser</button>
                    </td>
                    <td colspan="2">
                        <button
                            id="rd_editing_build_geometry_from_references" class="btn road_network_btn"
                            style="width:100%;"
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

        // Set the road_code from Lizmap editing form
        const roadCodeInput = document.getElementById('rd_editing_road_code');
        if (roadCodeInput) {
            const editingRoadCodeInput = editingForm ? editingForm.querySelector('input[name="road_code"]') : null;
            if (editingRoadCodeInput) {
                // console.log('## Set road_code from Lizmap editing form', editingRoadCodeInput.value);
                roadCodeInput.value = editingRoadCodeInput.value;
            }
        }

        // Catch the click event of the button-like span
        const form = document.getElementById('road_network_editing_form');
        const el = form.querySelectorAll('span.road_network_go_to_tip').forEach(el => {
            el.addEventListener('click', evt => {
                const isStart = el.id.startsWith('rd_editing_go_to_start_edge');
                const tipType = el.classList.contains('route_tip') ? 'route' : 'edge';
                // Get the references for the start or the end edge references of the given reference
                evt.stopPropagation();
                evt.preventDefault();
                const roadCode = document.getElementById('rd_editing_road_code').value;
                const markerCode = isStart ? document.getElementById('rd_editing_marker_code').value : document.getElementById('rd_editing_marker_code_end').value;
                const abscissa = isStart ? document.getElementById('rd_editing_abscissa').value : document.getElementById('rd_editing_abscissa_end').value;
                const start_or_end = isStart ? 'start' : 'end';
                const tip_type = tipType;

                const actionParams = {
                    road_code: roadCode,
                    marker_code: markerCode,
                    abscissa: abscissa,
                    start_or_end: start_or_end,
                    tip_type: tip_type
                };
                // Run the Lizmap action to get the start or end edge references from the given reference
                lizMap.mainLizmap.action.runLizmapAction(
                    'get_edge_start_or_end_tip_from_references',
                    'layer',
                    ROAD_EDGES_LAYER_ID,
                    null,
                    null,
                    actionParams
                );

            });
        })

        // We catch the references form submit event instead of the click event of the button
        form.addEventListener('submit', evt => {
            // console.log('FORM SUBMITTED', evt);
            evt.stopPropagation();
            evt.preventDefault();

            // Hide the mini-dock if it is open
            const roadMenu = document.querySelector('#mapmenu li.nav-minidock.road_network');
            if (roadMenu.classList.contains('active')) {
                roadMenu.querySelector('a').click();
            }

            // Get the clicked button
            const clickedButton = evt.submitter;
            // console.log('Clicked button', clickedButton);
            if (!clickedButton) return false;

            if (clickedButton.id == 'rd_editing_reset_geometry') {
                // Reset the geometry of the feature being edited
                const resetButton = document.getElementById('edition-geomtool-restart-drawing');
                if (resetButton) {
                    // Click the reset button to reset the geometry of the feature being edited
                    resetButton.click();
                    // Reset the form values
                    document.getElementById('road_network_editing_form').reset();
                } else {
                    console.error('Cannot find the reset button for the editing geometry');
                }
            }
            else if (clickedButton.id == 'rd_editing_build_geometry_from_references') {
                // Get the geometry from the form references
                // depending on the type of the feature being edited (point or line),
                // we need different references
                let setGeometry = setEditingGeometryFromReferences();
                if (setGeometry) {
                    // Display a message to the user
                    displayMessage(
                        'La géométrie a bien été créée à partir des références.',
                        'info',
                        3000
                    );
                }
            }

            // Prevent the form from being submitted to the server
            return false;
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

        // Get Lizmap editing form road_code value to use it in the Lizmap action parameters
        const editingForm = document.querySelector('form#jforms_view_edition');
        const editingFormRoadCodeElt = editingForm ? editingForm.querySelector('input[name="road_code"]') : null;
        let editingFormRoadCode = '';
        let searchedRoadCode = '';
        if (editingFormRoadCodeElt) {
            // console.log('onEditing - Set road_code from Lizmap editing form', editingFormRoadCodeElt.value);
            // Set the road_code in the form to the value from the Lizmap editing form
            editingFormRoadCode = editingFormRoadCodeElt.value;
            searchedRoadCode = editingFormRoadCode;
        }
        const roadCodeElement = document.getElementById('rd_editing_road_code');
        if (roadCodeElement) {
            if (
                (searchedRoadCode && roadCodeElement.value && roadCodeElement.value != searchedRoadCode)
                ||
                (!searchedRoadCode && roadCodeElement.value)
            ) {
                searchedRoadCode = roadCodeElement.value;
                // console.log('onEditing - new form wins', searchedRoadCode);
            }
        }
        // console.log('onEditing - Get references from geometry modification, geometry type:', geometryType, 'road_code:', searchedRoadCode);

        // Depending on the geometry type, we get the coordinates of the point or the first and last vertex of the line
        if (geometryType == 'point') {
            // Get the point coordinates
            const wkt = `POINT(${clonedGeom.x} ${clonedGeom.y})`;
            // console.log('Feature modified, point geometry WKT:', wkt);
            lizMap.mainLizmap.action.runLizmapAction(
                'get_references_from_point',
                'layer',
                ROAD_EDGES_LAYER_ID,
                null,
                wkt,
                {"vertex_number": 0, "road_code": searchedRoadCode}
            );

        } else if (geometryType == 'line') {
            // It depends on the type : MULTI or simple
            let firstWkt = '';
            let lastWkt = '';
            if (clonedGeom.CLASS_NAME == 'OpenLayers.Geometry.MultiLineString') {
                // console.log('Feature modified, line geometry is a MultiLineString with', clonedGeom.components.length, 'components');
                // Get the line first and last vertex coordinates
                // We use the third coordinate to indicate the order of the vertex: 0 for first, 1 for last
                const firstVertex = clonedGeom.components[0].components[0];
                const lastVertex = clonedGeom.components[clonedGeom.components.length - 1].components[clonedGeom.components[clonedGeom.components.length - 1].components.length - 1];
                firstWkt = `POINT(${firstVertex.x} ${firstVertex.y})`;
                lastWkt = `POINT(${lastVertex.x} ${lastVertex.y})`;
            } else {
                // console.log('Feature modified, line geometry is a LineString with', clonedGeom.components.length, 'components');
                // Get the line first and last vertex coordinates
                // We use the third coordinate to indicate the order of the vertex: 0 for first, 1 for last
                const firstVertex = clonedGeom.components[0];
                const lastVertex = clonedGeom.components[clonedGeom.components.length - 1];
                firstWkt = `POINT(${firstVertex.x} ${firstVertex.y})`;
                lastWkt = `POINT(${lastVertex.x} ${lastVertex.y})`;
            }

            if (!firstWkt || !lastWkt) {
                // console.log('Feature modified, line geometry is invalid, cannot get first and last vertex WKT');
                return;
            }
            console.log('Feature modified, line geometry first vertex WKT:', firstWkt, 'last vertex WKT:', lastWkt);

            // console.log('Feature modified, line first vertex WKT:', firstWkt);
            lizMap.mainLizmap.action.runLizmapAction(
                'get_references_from_point',
                'layer',
                ROAD_EDGES_LAYER_ID,
                null,
                firstWkt,
                {"vertex_number": 0, "road_code": searchedRoadCode}
            );
            // console.log('Feature modified, line last vertex WKT:', lastWkt);
            lizMap.mainLizmap.action.runLizmapAction(
                'get_references_from_point',
                'layer',
                ROAD_EDGES_LAYER_ID,
                null,
                lastWkt,
                {"vertex_number": 1, "road_code": searchedRoadCode}
            );
        }

        // Do no nothing if the geometry has been build from the WKT received
        // It prevents the infinite loop when the user modifies the geometry and the references are filled in the form
        if (ROAD_WAIT_FOR_GEOMETRY_IN_EDITING_FORM) {
            return;
        }

        // Create the geometry once the references have been filled in the form.
        // We wait for the references to be filled in the form before getting the geometry,
        // because the get_references_from_point action is asynchronous
        // and we cannot guarantee that the references will be filled in the form before we get the geometry.
        // We do it only if the linestring
        const refForm = document.getElementById('road_network_editing_form');
        if (!refForm) {
            // console.log('References form not found, cannot get the geometry from the references');
            return;
        }

        // Recreate the geometry every time it has been modified, but only if the references have been filled in the form.
        // Only loop 10 times because the loop will not end if the user modifies the geometry with a node outside the 50m
        const maxAttempts = 20;
        let attempts = 0;
        const intervalId = setInterval(() => {
            // Abort if we reach the maximum number of attempts
            if (attempts >= maxAttempts) {
                clearInterval(intervalId);
                // console.log('Max attempts reached, aborting');

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
                // console.log('References filled in the form, now we can get the geometry from the references');
                ROAD_EDITION_FORM_AUTO_GEOM = true;
                setEditingGeometryFromReferences();
            }
            // else {
            //     console.log('Waiting for references to be filled in the form before getting the geometry');
            //     console.log('max Attempts left:', maxAttempts-attempts);
            // }
            attempts++;
        }, 100);

    }

    /**
     * Add split buttons for a given linestring layer feature popup
     *
     * @param {HTMLElement} element Popup element
     * @param {string} layerId The layer ID
     * @param {number} featureId The feature ID
     */
    function addSplitPopupButton(element, layerId, featureId) {
        // Do not use the buttons for the edges layer
        if (layerId == ROAD_EDGES_LAYER_ID) return true;

        // Get Lizmap layer configuration to check the geometry type of the layer
        const getLayerConfig = lizMap.getLayerConfigById(layerId);
        if (!getLayerConfig) return true;
        if (getLayerConfig[1].geometryType != 'line') return true;

        // Get action configuration
        const actionConfig = lizMap.mainLizmap.action.getActionItemByName('split_line_by_point', 'feature');
        if (!actionConfig) {
            return true;
        }
        const actionLayers = actionConfig.layers;
        if (!actionLayers || !actionLayers.includes(layerId)) {
            return true;
        }

        // Add a button to allow the user to split the line at the clicked point
        const button = document.createElement('button');
        button.classList.add('btn', 'road_network_btn');
        button.innerHTML = actionConfig.title;
        button.setAttribute('title', actionConfig.title);
        button.dataset.actionName = 'split_line_by_point';
        button.addEventListener('click', function(evt) {
            evt.preventDefault();
            evt.stopPropagation();

            // Confirm with the user
            if ('confirm' in actionConfig) {
                if (!confirm(actionConfig.confirm)) {
                    return;
                }
            }

            // Get action feature geometry
            const getActionFeatures = lizMap.mainLizmap.action.actionLayer.getSource().getFeatures();
            if (!getActionFeatures || getActionFeatures.length == 0) {
                displayMessage(
                    `Aucun point n'a été trouvé. Veuillez ouvrir le panneau "Graphe routier", puis cliquer un point sur la ligne.`,
                    'info',
                    3000
                );
                return;
            }
            const feature = getActionFeatures[0];
            const geometry = feature.getGeometry();
            const WKTformat = new lizMap.ol.format.WKT();
            const projOptions = {
                featureProjection: lizMap.mainLizmap.projection,
                dataProjection: 'EPSG:4326'
            };
            const wkt = WKTformat.writeGeometry(geometry, projOptions);

            // Run Lizmap action
            lizMap.mainLizmap.action.runLizmapAction(
                'split_line_by_point',
                'feature',
                layerId,
                featureId,
                wkt
            );
        });
        element.parentNode.appendChild(button);

        // Hide original toolbar action button with value beginning by split_line_by_point
        const originalBt = element.parentNode.querySelector('button[value^="split_line_by_point"]');
        if (originalBt) originalBt.remove();

        return true;
    }

    /**
     * Add merge button for a given linestring layer feature popup
     *
     * @param {HTMLElement} element Popup element
     * @param {string} layerId The layer ID
     * @param {number} featureId The feature ID
     */
    function addMergePopupButton(element, layerId, featureId) {
        // Do not use the buttons for the edges layer
        if (layerId == ROAD_EDGES_LAYER_ID) return true;

        // Get Lizmap layer configuration to check the geometry type of the layer
        const getLayerConfig = lizMap.getLayerConfigById(layerId);
        if (!getLayerConfig) return true;
        if (getLayerConfig[1].geometryType != 'line') return true;

        // Get action configuration
        const actionConfig = lizMap.mainLizmap.action.getActionItemByName('merge_selected_lines', 'layer');
        if (!actionConfig) {
            return true;
        }
        const actionLayers = actionConfig.layers;
        if (!actionLayers || !actionLayers.includes(layerId)) {
            return true;
        }

        // Get current step
        let currentStep = 0;
        if (ROAD_MERGE_LINES_STATE.feature_id_a > 0) {
            currentStep = (ROAD_MERGE_LINES_STATE.feature_id_b > 0) ? 2 : 1;
        }

        // Do not add a button if the first feature has already been chosen
        if (currentStep > 0 && layerId != ROAD_MERGE_LINES_STATE.layer_id) return true;

        // Add a button to allow the user to select then the selected features
        const button = document.createElement('button');
        button.classList.add('btn', 'road_network_btn');
        button.dataset.actionName = 'merge_selected_lines';
        button.disabled = false;
        if (currentStep == 2) {
            button.disabled = true;
        }
        button.innerText = ROAD_MERGE_LINES_BUTTONS[currentStep].text;
        button.setAttribute('title', ROAD_MERGE_LINES_BUTTONS[currentStep].title);
        button.classList.toggle('active', false);
        button.dataset.currentStep = currentStep;
        button.dataset.layerId = layerId;
        button.dataset.featureId = parseInt(featureId);
        if (featureId == ROAD_MERGE_LINES_STATE.feature_id_a) {
            button.innerText = ROAD_MERGE_LINES_BUTTONS[3].text;
            button.title = ROAD_MERGE_LINES_BUTTONS[3].title;
            button.classList.toggle('active', true);
        }

        button.addEventListener('click', function(evt) {
            evt.preventDefault();
            evt.stopPropagation();

            const clickedButton = evt.currentTarget;
            const clickedLayerId = clickedButton.dataset.layerId;
            const clickedFeatureId = clickedButton.dataset.featureId;
            // console.log('DEBUT ROAD_MERGE_LINES_STATE = ', ROAD_MERGE_LINES_STATE);

            // Get current step when clicking
            let stepBeforeClick = 0;
            if (ROAD_MERGE_LINES_STATE.feature_id_a > 0) {
                stepBeforeClick = (ROAD_MERGE_LINES_STATE.feature_id_b > 0) ? 2 : 1;
            }

            if (stepBeforeClick == 0) {
                // Set state
                ROAD_MERGE_LINES_STATE.layer_id = clickedButton.dataset.layerId;
                ROAD_MERGE_LINES_STATE.feature_id_a = parseInt(clickedButton.dataset.featureId);
                ROAD_MERGE_LINES_STATE.feature_id_b = null;

                // Change button
                clickedButton.innerText = ROAD_MERGE_LINES_BUTTONS[3].text;
                clickedButton.title = ROAD_MERGE_LINES_BUTTONS[3].title;
                clickedButton.classList.toggle('active', true);

                // Change other buttons of the same action and layer
                document.querySelectorAll('button.road_network_btn[data-action-name="merge_selected_lines"]').forEach(otherButton => {
                    if (otherButton.dataset.layerId != clickedLayerId) return true;
                    if (otherButton.dataset.featureId == clickedFeatureId) return true;
                    otherButton.innerText = ROAD_MERGE_LINES_BUTTONS[1].text;
                    otherButton.title = ROAD_MERGE_LINES_BUTTONS[1].title;
                    otherButton.classList.toggle('active', false);
                });

            } else if (stepBeforeClick == 1) {
                // Set state depending on the feature Id clicked
                if (clickedButton.dataset.featureId == ROAD_MERGE_LINES_STATE.feature_id_a) {
                    // If it is the same, cancel the operation
                    // Set state
                    ROAD_MERGE_LINES_STATE.layer_id = null;
                    ROAD_MERGE_LINES_STATE.feature_id_a = null;
                    ROAD_MERGE_LINES_STATE.feature_id_b = null;
                    // Change button
                    clickedButton.innerText = ROAD_MERGE_LINES_BUTTONS[0].text;
                    clickedButton.title = ROAD_MERGE_LINES_BUTTONS[0].title;
                    clickedButton.classList.toggle('active', false);
                    // Change other buttons of the same action and layer
                    document.querySelectorAll('button.road_network_btn[data-action-name="merge_selected_lines"]').forEach(otherButton => {
                        if (otherButton.dataset.layerId != clickedLayerId) return true;
                        if (otherButton.dataset.featureId == clickedFeatureId) return true;
                        otherButton.innerText = ROAD_MERGE_LINES_BUTTONS[0].text;
                        otherButton.title = ROAD_MERGE_LINES_BUTTONS[0].title;
                        otherButton.classList.toggle('active', false);
                    });
                } else {
                    // Run the action
                    ROAD_MERGE_LINES_STATE.feature_id_b = parseInt(clickedButton.dataset.featureId);

                    // Change button
                    clickedButton.innerText = ROAD_MERGE_LINES_BUTTONS[2].text;
                    clickedButton.title = ROAD_MERGE_LINES_BUTTONS[2].title;
                    clickedButton.disabled = true;
                    clickedButton.classList.toggle('active', true);
                    // Change other buttons of the same action and layer
                    document.querySelectorAll('button.road_network_btn[data-action-name="merge_selected_lines"]').forEach(otherButton => {
                        if (otherButton.dataset.layerId != clickedLayerId) return true;
                        otherButton.innerText = ROAD_MERGE_LINES_BUTTONS[2].text;
                        otherButton.title = ROAD_MERGE_LINES_BUTTONS[2].title;
                        otherButton.disabled = true;
                        otherButton.classList.toggle('active', true);
                    });

                    // Confirm with the user
                    if ('confirm' in actionConfig) {
                        if (!confirm(actionConfig.confirm)) {
                            // If it is the same, cancel the operation
                            // Set state
                            ROAD_MERGE_LINES_STATE.layer_id = null;
                            ROAD_MERGE_LINES_STATE.feature_id_a = null;
                            ROAD_MERGE_LINES_STATE.feature_id_b = null;
                            // Change button
                            clickedButton.innerText = ROAD_MERGE_LINES_BUTTONS[0].text;
                            clickedButton.title = ROAD_MERGE_LINES_BUTTONS[0].title;
                            clickedButton.classList.toggle('active', false);
                            // Change other buttons of the same action and layer
                            document.querySelectorAll('button.road_network_btn[data-action-name="merge_selected_lines"]').forEach(otherButton => {
                                if (otherButton.dataset.layerId != clickedLayerId) return true;
                                if (otherButton.dataset.featureId == clickedFeatureId) return true;
                                otherButton.innerText = ROAD_MERGE_LINES_BUTTONS[0].text;
                                otherButton.title = ROAD_MERGE_LINES_BUTTONS[0].title;
                                otherButton.classList.toggle('active', false);
                            });

                            return;
                        }
                    }

                    // Run Lizmap action
                    const actionOptions = {
                        'feature_id_a': ROAD_MERGE_LINES_STATE.feature_id_a,
                        'feature_id_b': ROAD_MERGE_LINES_STATE.feature_id_b
                    };
                    lizMap.mainLizmap.action.runLizmapAction(
                        'merge_selected_lines',
                        'layer',
                        clickedLayerId,
                        null,
                        null,
                        actionOptions
                    );
                }
            } else {
                // Do nothing
                // console.log(`layerId = ${clickedButton.dataset.layerId}, featureId = ${parseInt(clickedButton.dataset.featureId)}, stepBeforeClick = ${stepBeforeClick}, on ne fait rien`);
            }
        });

        element.parentNode.appendChild(button);

        return true;
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
