import * as vscode from 'vscode';
import { ChromaTreeProvider } from '../providers/chromaTreeProvider';

export function registerTenantCommands(context: vscode.ExtensionContext, provider: ChromaTreeProvider) {
  context.subscriptions.push(
    vscode.commands.registerCommand('chromadb.createTenant', async () => {
      const name = await vscode.window.showInputBox({ prompt: 'New tenant name' });
      if (!name) { return; }
      await provider.client.createTenant(name);
      vscode.window.showInformationMessage(`Tenant '${name}' created`);
      provider.refresh();
    })
  );

}
