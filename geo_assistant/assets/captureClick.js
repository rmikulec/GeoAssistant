// assets/captureClick.js
document.addEventListener('DOMContentLoaded', () => {
  const gd = document.getElementById('map-graph');

  function attachMapClick(attempt = 0) {
    // 1. Wait until Plotly has initialized the internal map
    const mapboxSubplot = gd?._fullLayout?.mapbox?._subplot;
    const map = mapboxSubplot?.map;
    if (!map) {
      if (attempt < 20) setTimeout(() => attachMapClick(attempt+1), 200);
      return;
    }

    // 2. Listen for native Mapbox‐GL clicks
    map.on('click', (e) => {
      // e.lngLat is a LngLat object with .lng and .lat
      const coords = { lon: e.lngLat.lng, lat: e.lngLat.lat };
      console.log('map click at', coords);

      // 3. Write to hidden DIV for Dash
      const out = document.getElementById('click-data');
      if (out) {
        out.textContent = JSON.stringify(coords);
        out.setAttribute('data-ts', Date.now());
      }
    });
  }

  attachMapClick();
});
