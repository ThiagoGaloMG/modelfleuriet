import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path' // Importa o módulo 'path' do Node.js

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  // Define a pasta raiz do projeto frontend.
  // Isso ajuda a resolver caminhos corretamente durante o build.
  root: '.', 
  build: {
    // Define a pasta de saída para 'dist', que o Flask espera.
    outDir: 'dist',
    // Limpa a pasta 'dist' antes de cada build para evitar arquivos antigos.
    emptyOutDir: true,
  },
  resolve: {
    alias: {
      // Cria um atalho '@' que aponta diretamente para a pasta 'src'.
      // Isso é útil para imports dentro dos seus componentes React (ex: import Component from '@/components/ui/button').
      '@': path.resolve(__dirname, './src'),
    },
  },
})
