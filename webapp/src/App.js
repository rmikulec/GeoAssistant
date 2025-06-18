import React, { useRef, useEffect, useState } from 'react';
import mapboxgl from 'mapbox-gl';
import ChatDrawer from './ChatDrawer';

// Set your Mapbox token here
mapboxgl.accessToken = 'YOUR_MAPBOX_ACCESS_TOKEN';

export default function App() {
  const mapContainer = useRef(null);
  const map = useRef(null);
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    if (map.current) return; // initialize map only once
    map.current = new mapboxgl.Map({
      container: mapContainer.current,
      style: 'https://api.maptiler.com/maps/streets/style.json?key=GET_YOUR_OWN',
      center: [-74.006, 40.7128],
      zoom: 10,
    });
  }, []);

  return (
    <div className="app">
      <div className="map-container" ref={mapContainer} />
      <button className="chat-toggle" onClick={() => setIsOpen(!isOpen)}>
        Chat
      </button>
      <ChatDrawer open={isOpen} onClose={() => setIsOpen(false)} />
    </div>
  );
}
