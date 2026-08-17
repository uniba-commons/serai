import { defineConfig } from 'vite-plus';

export default defineConfig({
  fmt: {},
  lint: {
    jsPlugins: [{ name: 'vite-plus', specifier: 'vite-plus/oxlint-plugin' }],
    rules: { 'vite-plus/prefer-vite-plus-imports': 'error' },
    options: { typeAware: true, typeCheck: true },
  },
  server: {
    // dev: proxy API and payload routes to the local serai agent
    proxy: {
      '/api': 'http://127.0.0.1:53241',
      '/artifacts': 'http://127.0.0.1:53241',
    },
  },
});
