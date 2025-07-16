import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path' // Importa o módulo 'path' do Node.js

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  // CORREÇÃO: Define a pasta raiz do projeto frontend explicitamente.
  // Isso garante que os caminhos no index.html sejam resolvidos corretamente.
  root: '.', 
  build: {
    // Define a pasta de saída para 'dist', que o Flask espera.
    outDir: 'dist',
    // Limpa a pasta 'dist' antes de cada build.
    emptyOutDir: true,
  },
  resolve: {
    alias: {
      // Cria um atalho '@' que aponta para a pasta 'src'.
      // Isso é útil para imports dentro dos seus componentes React.
      '@': path.resolve(__dirname, './src'),
    },
  },
})
