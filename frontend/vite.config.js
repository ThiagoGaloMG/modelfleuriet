import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  
  // CORREÇÃO: Define o 'base' para caminhos relativos.
  // Isso garante que os links para os arquivos JS e CSS no index.html gerado
  // sejam relativos, o que é mais robusto para deploy.
  base: './',

  build: {
    // A pasta de saída continua sendo 'dist'.
    outDir: 'dist',
    // Garante que a pasta 'dist' seja limpa antes de cada build.
    emptyOutDir: true,
  },
})
