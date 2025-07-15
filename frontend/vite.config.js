import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist', // Onde os arquivos de build ser�o gerados
  },
  server: {
    proxy: {
      // Proxy para redirecionar chamadas de API para o backend Flask
      '/api': {
        target: 'http://localhost:8080', // Endere�o do seu backend Flask em desenvolvimento (porta 8080 conforme flask_app.py)
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '/api'), // Garante que o path /api seja mantido
      },
    },
  },
});