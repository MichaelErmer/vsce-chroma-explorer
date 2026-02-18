import * as vscode from 'vscode';
import { ChromaTreeProvider } from '../providers/chromaTreeProvider';
import { RecordItem } from '../treeItems/RecordItem';
import { RecordPanel } from '../panels/recordPanel';

export function registerRecordCommands(context: vscode.ExtensionContext, provider: ChromaTreeProvider) {
  context.subscriptions.push(
    vscode.commands.registerCommand('chromadb.viewRecord', async (node: RecordItem) => {
      if (!node) { return; }
      const cfg = vscode.workspace.getConfiguration('chromadb');
      const tenant = cfg.get<string>('tenant') || 'default_tenant';
      const database = cfg.get<string>('database') || 'default_database';
      const collection = node.parentCollectionName || (await vscode.window.showInputBox({ prompt: 'Collection for record', value: '' })) || 'example_collection';
      const record = await provider.client.getRecord(tenant, database, collection, node.id);
      RecordPanel.createOrShow(context.extensionUri, record || { id: node.id, document: node.description });

      // attach message handler to the active panel for save/delete actions
      const panel = (RecordPanel as any).currentPanel as vscode.WebviewPanel | undefined;
      if (panel) {
        const disposable = panel.webview.onDidReceiveMessage(async (msg) => {
          try {
            if (msg.type === 'save') {
              const payload = JSON.parse(msg.payload);
              // ensure id is present
              const id = payload.id || node.id;
              await provider.client.updateRecord(tenant, database, collection, { id, document: payload.document, metadata: payload.metadata, embedding: payload.embedding });
              vscode.window.showInformationMessage(`Record ${id} updated`);
              provider.refresh();
            }
            if (msg.type === 'delete') {
              await provider.client.deleteRecord(tenant, database, collection, node.id);
              vscode.window.showInformationMessage(`Record ${node.id} deleted`);
              provider.refresh();
              panel.dispose();
            }
          } catch (err) {
            vscode.window.showErrorMessage(String(err));
          }
        });

        // dispose the listener when panel is closed
        panel.onDidDispose(() => disposable.dispose());
      }
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('chromadb.addRecord', async (node) => {
      const cfg = vscode.workspace.getConfiguration('chromadb');
      const tenant = cfg.get<string>('tenant') || 'default_tenant';
      const database = cfg.get<string>('database') || 'default_database';
      const collection = node && node.name ? node.name.split(' ')[0] : await vscode.window.showInputBox({ prompt: 'Collection name' });
      if (!collection) { return; }
      const id = await vscode.window.showInputBox({ prompt: 'Record ID (leave empty to auto-generate)' });
      const doc = await vscode.window.showInputBox({ prompt: 'Document / text' });
      // optional embedding JSON input (users can paste a JSON array)
      const embInput = await vscode.window.showInputBox({ prompt: 'Embedding JSON (optional, e.g. [0.1,0.2])' });
      let embedding: number[] | undefined = undefined;
      if (embInput) {
        try { const parsed = JSON.parse(embInput); if (Array.isArray(parsed)) embedding = parsed as number[]; } catch (e) { /* ignore */ }
      }
      // ensure a valid embedding (server requires at least one dimension) — use workspace default when not provided
      if (!Array.isArray(embedding) || embedding.length === 0) {
        const cfg = vscode.workspace.getConfiguration('chromadb');
        const def = cfg.get<number[]>('defaultEmbedding') || [0.0];
        embedding = Array.isArray(def) && def.length > 0 ? def : [0.0];
      }
      await provider.client.addRecord(tenant, database, collection, { id: id || undefined, document: doc || '', embedding });
      vscode.window.showInformationMessage('Record added');
      provider.refresh();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('chromadb.updateRecord', async (node: RecordItem) => {
      if (!node) { return; }
      const cfg = vscode.workspace.getConfiguration('chromadb');
      const tenant = cfg.get<string>('tenant') || 'default_tenant';
      const database = cfg.get<string>('database') || 'default_database';
      const collection = node.parentCollectionName || await vscode.window.showInputBox({ prompt: 'Collection for record', value: '' }) || 'example_collection';
      const doc = await vscode.window.showInputBox({ prompt: 'New document / text', value: typeof node.description === 'string' ? node.description : '' });
      if (doc === undefined) { return; }
      await provider.client.updateRecord(tenant, database, collection, { id: node.id, document: doc });
      vscode.window.showInformationMessage(`Record ${node.id} updated`);
      provider.refresh();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('chromadb.deleteRecord', async (node: RecordItem) => {
      if (!node) { return; }
      const cfg = vscode.workspace.getConfiguration('chromadb');
      const tenant = cfg.get<string>('tenant') || 'default_tenant';
      const database = cfg.get<string>('database') || 'default_database';
      const collection = node.parentCollectionName || 'example_collection';
      const ok = await vscode.window.showWarningMessage(`Delete record '${node.id}' from '${collection}'?`, { modal: true }, 'Delete');
      if (ok !== 'Delete') { return; }
      await provider.client.deleteRecord(tenant, database, collection, node.id);
      vscode.window.showInformationMessage(`Record ${node.id} deleted`);
      provider.refresh();
    })
  );
}
