import { defineConfig, Options } from "tsup";

const common: Options = {
  outDir: "lib",
  sourcemap: true,
  minify: false,
  splitting: true,
};

export default defineConfig([
  {
    ...common,
    entryPoints: ["src/main/index.ts"],
    format: ["esm"],
    outDir: common.outDir + "/main",
  }, {
    ...common,
    entryPoints: ["src/preload/index.ts"],
    format: ["cjs"],
    outDir: common.outDir + "/preload",
  },
]);