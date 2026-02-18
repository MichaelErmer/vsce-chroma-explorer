# ChromaDB Viewer — VS Code extension

Manage and inspect ChromaDB (tenants, databases, collections and vectors) directly from VS Code.

Highlights
- Browse Tenants → Databases → Collections → Records (TreeView)
- Create / rename / delete tenants, databases, collections and records
- View and edit record documents, metadata and embeddings in a Webview panel
- Run similarity queries and inspect results
- Integrated Docker seeder + end-to-end integration tests

Features
- Tree-based explorer with context-menu commands
- React + Vite webview for record detail editing and query UI
- Securely store API key in VS Code SecretStorage
- Integration tests that run against a local Docker Chroma instance

![screenshot 1](./resources/images/screenshot1.png)

![screenshot 2](./resources/images/screenshot2.png)

Install & run (dev)
1. npm install
2. npm run compile
3. Press F5 in VS Code (Start Debugging) — the Extension Development Host opens
4. Open Activity Bar → **ChromaDB** → `Explorer`

Connect to a Chroma server
- Command Palette → `ChromaDB: Connect` (enter host/port/tenant/database)
- Defaults can be set in Settings: `chromadb.host`, `chromadb.port`, `chromadb.tenant`, `chromadb.database`

Disclaimer: This project is independently developed and the author is not affiliated with "Chroma Inc.".

Local test environment (Docker)
- Start and seed: npm run test:setup
- Teardown: npm run test:teardown

Run integration tests
- npm test  (runs tests inside VS Code Test Runner)

Commands (Command Palette / context menus)
- ChromaDB: Connect
- ChromaDB: Refresh
- ChromaDB: Create Tenant / Delete Tenant
- ChromaDB: Create Database / Delete Database
- ChromaDB: Create Collection / Rename Collection / Delete Collection
- ChromaDB: Add Record / View Record / Update Record / Delete Record
- ChromaDB: Query Collection

Development notes
- Webview UI is under `webview-ui/` (React + Vite)
- Tests: `src/test/suite` (Mocha + @vscode/test-electron)
- Client wrapper: `src/client/chromaClient.ts` (HTTP REST wrapper + mock fallback)

Contributing
- Fork, add tests, open a PR. CI integration can be added on request.

License
- MIT

---
