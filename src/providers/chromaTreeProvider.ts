import * as vscode from 'vscode';
import { ChromaDbClient } from '../client/chromaClient';
import { TenantItem } from '../treeItems/TenantItem';
import { DatabaseItem } from '../treeItems/DatabaseItem';
import { CollectionItem } from '../treeItems/CollectionItem';
import { RecordItem } from '../treeItems/RecordItem';

export class ChromaTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
  private _onDidChangeTreeData: vscode.EventEmitter<vscode.TreeItem | undefined | null | void> = new vscode.EventEmitter();
  readonly onDidChangeTreeData: vscode.Event<vscode.TreeItem | undefined | null | void> = this._onDidChangeTreeData.event;

  public client = new ChromaDbClient();
  private context: vscode.ExtensionContext;

  constructor(context: vscode.ExtensionContext) {
    this.context = context;
  }

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: vscode.TreeItem): vscode.TreeItem | Thenable<vscode.TreeItem> {
    return element;
  }

  private withTimeout<T>(p: Promise<T>, ms = 5000): Promise<T> {
    return Promise.race([p, new Promise<T>((_, rej) => setTimeout(() => rej(new Error('operation timed out')), ms))]);
  }

  async getChildren(element?: vscode.TreeItem): Promise<vscode.TreeItem[]> {
    try {
      // top-level: if not connected show a connect prompt instead of mock data
      if (!element) {
        if (!this.client.isConnected()) {
          const { NotConnectedItem } = await import('../treeItems/NotConnectedItem');
          return [new NotConnectedItem()];
        }

        // connected → configured tenant is the root
        const cfg = vscode.workspace.getConfiguration('chromadb');
        const tenant = cfg.get<string>('tenant') || 'default_tenant';
        return [new TenantItem(tenant)];
      }

      // tenant -> databases
      if (element instanceof TenantItem) {
        const tenantName = element.name;
        const dbs = await this.withTimeout(this.client.listDatabases(tenantName));
        return dbs.map((d: string) => new DatabaseItem(d));
      }

      // database -> collections
      if (element instanceof DatabaseItem) {
        const dbName = element.name;
        // use default tenant from settings for now
        const cfg = vscode.workspace.getConfiguration('chromadb');
        const tenant = cfg.get<string>('tenant') || 'default_tenant';
        const cols = await this.withTimeout(this.client.listCollections(tenant, dbName));
        return cols.map((c: any) => new CollectionItem(c.name, c.count || 0, dbName));
      }

      // collection -> records
      if (element instanceof CollectionItem) {
        const cfg = vscode.workspace.getConfiguration('chromadb');
        const tenant = cfg.get<string>('tenant') || 'default_tenant';
        const database = cfg.get<string>('database') || 'default_database';
        const collName = element.name.split(' ')[0]; // name (count)
        const recs = await this.withTimeout(this.client.listRecords(tenant, database, collName, 50, 0));
        return recs.map((r: any) => new RecordItem(r.id, r.document, collName));
      }

      return [];
    } catch (err: any) {
      console.error('ChromaTreeProvider.getChildren failed:', err);
      vscode.window.showErrorMessage(`ChromaDB explorer: ${String(err.message || err)}`);
      const item = new vscode.TreeItem('Error loading ChromaDB — see Output for details', vscode.TreeItemCollapsibleState.None);
      item.iconPath = new vscode.ThemeIcon('warning');
      return [item];
    }
  }
}
