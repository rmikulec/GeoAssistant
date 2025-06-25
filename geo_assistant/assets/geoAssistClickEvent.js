// assets/custom.js
(function waitAndAttach(attempt = 0) {
  const plotlyDiv = document.querySelector('.js-plotly-plot');
  const container = document.getElementById('map-graph-container');
  if (plotlyDiv && plotlyDiv._fullLayout?.mapbox?._subplot?.map) {
    const map = plotlyDiv._fullLayout.mapbox._subplot.map;
    map.on('click', mbEvent => {
      const lon = mbEvent.lngLat.lng, lat = mbEvent.lngLat.lat;
      console.log('🌐 mapbox click:', { lon, lat });

      // dispatch on the graph container and bubble up to the EventListener div
      container.dispatchEvent(new CustomEvent("plotlyMapClick", {
        detail: { lon, lat },
        bubbles: true,
        composed: true
      }));
    });
  } else if (attempt < 40) {
    setTimeout(() => waitAndAttach(attempt + 1), 150);
  } else {
    console.error("Could not find Plotly+Mapbox or map-graph container");
  }
})();