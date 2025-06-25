// assets/custom.js
(function waitAndAttach(attempt = 0) {
  const plotlyDiv = document.querySelector('.js-plotly-plot');
  const container = document.getElementById('map-graph-container');
  if (plotlyDiv && plotlyDiv._fullLayout?.mapbox?._subplot?.map) {
    const map = plotlyDiv._fullLayout.mapbox._subplot.map;
    map.on('click', mbEvent => {
      const lon = mbEvent.lngLat.lng, lat = mbEvent.lngLat.lat;
      const x = mbEvent.point.x, y = mbEvent.point.y
      console.log('🌐 mapbox click:', { lon, lat });

      fetch(`http://127.0.0.1:8000/query/lat-long/${lat}/${lon}`)
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();     // <-- return the JSON‐parsing promise
      })
      .then(data => {
        // data is the fully parsed JSON object
        console.log('Got JSON data:', data);

        container.dispatchEvent(new CustomEvent("plotlyMapClick", {
          detail: { lon, lat, x, y, results: data },
          bubbles: true,
          composed: true
        }));
      })


    });
  } else if (attempt < 40) {
    setTimeout(() => waitAndAttach(attempt + 1), 150);
  } else {
    console.error("Could not find Plotly+Mapbox or map-graph container");
  }
})();