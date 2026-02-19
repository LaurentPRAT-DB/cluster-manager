import { TanStackRouterVite } from "@tanstack/router-plugin/vite";
import react from "@vitejs/plugin-react";
import { execSync } from "child_process";
import path from "path";
import { defineConfig } from "vite";

// Get git info for build metadata
const getGitInfo = () => {
  try {
    const commitHash = execSync("git rev-parse --short HEAD").toString().trim();
    const commitDate = execSync("git log -1 --format=%cd --date=short").toString().trim();
    return { commitHash, commitDate };
  } catch {
    return { commitHash: "dev", commitDate: new Date().toISOString().split("T")[0] };
  }
};

const { commitHash, commitDate } = getGitInfo();

export default defineConfig({
  plugins: [
    TanStackRouterVite({
      routesDirectory: "./routes",
      generatedRouteTree: "./routeTree.gen.ts",
    }),
    react(),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./"),
    },
  },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "../__dist__",
    emptyOutDir: true,
  },
  define: {
    __APP_NAME__: JSON.stringify("Cluster Manager"),
    __APP_VERSION__: JSON.stringify("1.6.0"),
    __BUILD_HASH__: JSON.stringify(commitHash),
    __BUILD_DATE__: JSON.stringify(commitDate),
  },
});
