import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import '@cloudflare/kumo/styles/standalone';
import './styles.css';

document.documentElement.dataset.mode = 'light';
document.documentElement.dataset.theme = 'kumo';
createRoot(document.getElementById('root')!).render(<React.StrictMode><App /></React.StrictMode>);
