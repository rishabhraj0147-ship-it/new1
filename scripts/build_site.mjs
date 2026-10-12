// WorldPulse build: turns the readable index.html into a minified, scrambled copy for publishing.
// Usage: node scripts/build_site.mjs <source index.html> <output folder>
import fs from "node:fs";
import path from "node:path";
import JavaScriptObfuscator from "javascript-obfuscator";
import { minify } from "html-minifier-terser";

const [src = "index.html", outDir = "_site"] = process.argv.slice(2);
let html = fs.readFileSync(src, "utf8");

// Scramble every inline <script> (names renamed, strings encoded, code flattened onto one line).
html = html.replace(/<script>([\s\S]*?)<\/script>/g, (_, code) => {
  const out = JavaScriptObfuscator.obfuscate(code, {
    compact: true,
    target: "browser",
    renameGlobals: false,
    identifierNamesGenerator: "hexadecimal",
    stringArray: true,
    stringArrayEncoding: ["base64"],
    stringArrayThreshold: 0.75,
    splitStrings: false,
    controlFlowFlattening: false,
    deadCodeInjection: false,
    selfDefending: false,
    disableConsoleOutput: false,
    unicodeEscapeSequence: false,
  }).getObfuscatedCode();
  return "<script>" + out + "</script>";
});

html = await minify(html, {
  collapseWhitespace: true, conservativeCollapse: false, removeComments: true,
  minifyCSS: true, minifyJS: false, removeRedundantAttributes: true, useShortDoctype: true,
});

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(path.join(outDir, "index.html"), html);
fs.writeFileSync(path.join(outDir, ".nojekyll"), "");
console.log(`Built ${outDir}/index.html (${(html.length / 1024).toFixed(0)} KB from ${(fs.readFileSync(src).length / 1024).toFixed(0)} KB)`);
