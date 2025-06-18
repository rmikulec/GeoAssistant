import React from 'react';
import Drawer from '@mui/material/Drawer';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';

function ChatDrawer({ open, onClose }) {
  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          width: '25vw',
          maxWidth: '400px',
          bgcolor: 'rgba(255, 255, 255, 0.9)'
        }
      }}
    >
      <Box p={2} sx={{ height: '100%' }}>
        <Typography variant="h6">Chat</Typography>
        {/* Chat content can be added here */}
      </Box>
    </Drawer>
  );
}

export default ChatDrawer;
