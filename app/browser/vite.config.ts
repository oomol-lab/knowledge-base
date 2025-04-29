import { defineConfig } from "vite";

export default defineConfig(({ mode }) => {
  let minify = true;
  if (mode !== "production") {
    minify = false;
  }
  return {
    define: {
      "process.env.NODE_ENV": JSON.stringify(mode),
    },
    css: {
      preprocessorOptions: {
        less: {
          math: "always",
        },
      },
    },
    build: {
      outDir: "../browser.lib",
      target: "chrome58",
      sourcemap: true,
      emptyOutDir: true,
      minify: minify,
      rollupOptions: {
        format: "iife",
        input: "src/index.tsx",
        output: {
          entryFileNames: "dist/index.js",
          assetFileNames: "dist/[name].[ext]",
        },
      },
    }
  };
});