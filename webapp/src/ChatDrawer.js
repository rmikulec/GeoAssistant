import React from 'react';

export default function ChatDrawer({ open, onClose }) {
  return (
    <div className={`drawer ${open ? 'open' : ''}`}>
      <div className="drawer-header">
        <button onClick={onClose}>Close</button>
      </div>
      <div className="drawer-body">
        <p>Chat content goes here.</p>
      </div>
    </div>
  );
}
