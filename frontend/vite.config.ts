import { defineConfig } from 'vite'

export default defineConfig({
    esbuild: {
        supported: {
            "top-level-await": true
        },
    },
    base: "/",
    preview: {
        port: 5173,
        strictPort: true,
    },
    server: {
        port: 5173,
        strictPort: true,
        host: true,
        origin: "http://0.0.0.0:5173",
    },
})

