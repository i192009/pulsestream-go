import {defineConfig} from 'vite';
import react from '@vitejs/plugin-react';
export default defineConfig({plugins:[react()],server:{port:13000,strictPort:true,proxy:{'/v1':'http://localhost:18081','/ws':{target:'ws://localhost:18082',ws:true}}}});
