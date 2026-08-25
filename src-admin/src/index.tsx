// this file is used only for the development simulation and is not part of the build
import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';

window.adapterName = 'sql';

const container = document.getElementById('root');
if (container) {
    const root = createRoot(container);
    root.render(
        <React.StrictMode>
            <App socket={{ port: 8081 }} />
        </React.StrictMode>,
    );
}
