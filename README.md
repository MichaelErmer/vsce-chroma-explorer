# ChromaDB Viewer — VS Code extension

A lightweight VS Code extension to explore and manage ChromaDB (tenants, databases, collections and vectors) directly from the editor.

## Preview

![ChromaDB Viewer — Explorer](resources/images/screenshot1.png)

![ChromaDB Viewer — Record view](resources/images/screenshot2.png)

## Key benefits
- Browse and manage tenants, databases, collections and records from the Activity Bar
- Add / view / update / delete records (documents, metadata, embeddings)
- Run similarity queries and inspect results inline
- Small, focused UI — no need to leave VS Code

## Quick usage
- Open: Activity Bar → **ChromaDB** → `Explorer`
- Connect: Command Palette → `ChromaDB: Connect` (or use connection panel)
- Primary commands are available from the tree item context menus and the Command Palette

## Settings (examples)
- `chromadb.host` — host for the Chroma server
- `chromadb.port` — server port
- `chromadb.tenant` — default tenant used by the explorer
- `chromadb.database` — default database used by the explorer

## Where to get it
- Install from the Visual Studio Marketplace or build a `.vsix` locally with `npx vsce package`.

## Disclaimer
This project is independently developed and **not affiliated** with Chroma Inc.

For development, testing and contribution instructions see `DEVELOPER.md`.
