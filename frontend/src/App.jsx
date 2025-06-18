import React, { useState } from 'react';
import { MapContainer, TileLayer } from 'react-leaflet';
import ChatDrawer from './ChatDrawer';
import Button from '@mui/material/Button';
import './App.css';

function App() {
  const [open, setOpen] = useState(false);

  return (
    <div className="app-container">
      <MapContainer
        center={[40.7128, -74.006]}
        zoom={13}
        style={{ height: '100vh', width: '100vw' }}
      >
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution="&copy; OpenStreetMap contributors"
        />
      </MapContainer>
      <Button
        variant="contained"
        onClick={() => setOpen(true)}
        style={{ position: 'absolute', top: 10, right: 10, zIndex: 1000 }}
      >
        Chat
      </Button>
      <ChatDrawer open={open} onClose={() => setOpen(false)} />
    </div>
  );
}

export default App;
