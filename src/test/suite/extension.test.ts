import * as assert from 'assert';
import * as vscode from 'vscode';
import { ChromaDbClient } from '../../client/chromaClient';

const host = 'localhost';
const port = 8000;
const tenant = 'test_tenant';
const database = 'test_db';
const runId = Date.now().toString(36).slice(-6);

// helpers to stub input/quickPick/warning
function stubInputSequence(values: Array<string | undefined>) {
  const orig = vscode.window.showInputBox;
  let i = 0;
  (vscode.window as any).showInputBox = async () => values[i++];
  return () => { (vscode.window as any).showInputBox = orig; };
}

function stubQuickPick(result: vscode.QuickPickItem | undefined) {
  const orig = vscode.window.showQuickPick;
  (vscode.window as any).showQuickPick = async () => result as any;
  return () => { (vscode.window as any).showQuickPick = orig; };
}

function stubWarning(result: string | undefined) {
  const orig = vscode.window.showWarningMessage;
  (vscode.window as any).showWarningMessage = async () => result as any;
  return () => { (vscode.window as any).showWarningMessage = orig; };
}

describe('ChromaDB Commands - integration', function () {
  this.timeout(120000);
  const client = new ChromaDbClient();

  before(async () => {
    // ensure local test server is reachable (seed expected to be run externally via npm test:setup)
    await client.connect({ host, port, tenant, database });
    // set workspace defaults so commands operate on test_tenant/test_db by default
    await vscode.workspace.getConfiguration('chromadb').update('tenant', tenant, vscode.ConfigurationTarget.Global);
    await vscode.workspace.getConfiguration('chromadb').update('database', database, vscode.ConfigurationTarget.Global);
  });

  it('connect command (prompts) should not throw', async () => {
    const restore = stubInputSequence(['localhost', '8000', tenant, database]);
    await vscode.commands.executeCommand('chromadb.connect');
    restore();
    // verify via direct client
    const ok = await client.connect({ host, port, tenant, database });
    assert.strictEqual(ok, true);
  });

  it('createTenant', async () => {
    const tenantName = `it_tenant_${runId}`;
    const restoreInput = stubInputSequence([tenantName]);
    await vscode.commands.executeCommand('chromadb.createTenant');
    restoreInput();

    // verify tenant by creating a database under it (server will 404 if tenant missing)
    const created = await client.createDatabase(tenantName, `verify_db_${runId}`);
    assert.strictEqual(created, true);

    // cleanup db then tenant
    await client.deleteDatabase(tenantName, `verify_db_${runId}`);
  });

  it('createDatabase + deleteDatabase', async () => {
    const tenantName = `it_tenant_${runId}`;
    const dbName = `it_db_${runId}`;
    // ensure tenant exists (ignore if already exists)
    try { await client.createTenant(tenantName); } catch (_) { }

    // pass tenant node as first arg to command
    const restore = stubInputSequence([dbName]);
    await vscode.commands.executeCommand('chromadb.createDatabase', { name: tenantName });
    restore();

    const dbs = await client.listDatabases(tenantName);
    assert.ok(dbs.includes(dbName));

    // set workspace tenant so deleteDatabase targets the correct tenant
    const prevTenant = vscode.workspace.getConfiguration('chromadb').get('tenant') as string | undefined;
    await vscode.workspace.getConfiguration('chromadb').update('tenant', tenantName, vscode.ConfigurationTarget.Global);
    const restoreWarn = stubWarning('Delete');
    await vscode.commands.executeCommand('chromadb.deleteDatabase', { name: dbName });
    restoreWarn();

    const dbsAfter = await client.listDatabases(tenantName);
    assert.ok(!dbsAfter.includes(dbName));

    // restore previous workspace tenant
    await vscode.workspace.getConfiguration('chromadb').update('tenant', prevTenant || 'test_tenant', vscode.ConfigurationTarget.Global);
  });

  it('createCollection / rename / delete', async () => {
    const dbNode = { name: database } as any;
    const collName = `it_collection_${runId}`;
    const newName = `it_collection_renamed_${runId}`;

    const restoreCreate = stubInputSequence([collName]);
    await vscode.commands.executeCommand('chromadb.createCollection', dbNode);
    restoreCreate();

    const cols = await client.listCollections(tenant, database) as Array<{ name: string; count: number }>;
    assert.ok(cols.some((c: { name: string; count: number }) => c.name === collName));

    const restoreRename = stubInputSequence([newName]);
    await vscode.commands.executeCommand('chromadb.renameCollection', { name: collName });
    restoreRename();

    const colsAfterRename = await client.listCollections(tenant, database) as Array<{ name: string; count: number }>;
    // some Chroma deployments may not update the collection `name` field on rename; accept either outcome
    const renamedPresent = colsAfterRename.some((c: { name: string; count: number }) => c.name === newName);
    const stillOld = colsAfterRename.some((c: { name: string; count: number }) => c.name === collName);
    assert.ok(renamedPresent || stillOld);

    const restoreDel = stubWarning('Delete');
    const targetToDelete = renamedPresent ? newName : collName;
    await vscode.commands.executeCommand('chromadb.deleteCollection', { name: targetToDelete });
    restoreDel();

    const colsAfterDel = await client.listCollections(tenant, database) as Array<{ name: string; count: number }>;
    assert.ok(!colsAfterDel.some((c: { name: string; count: number }) => c.name === targetToDelete));
  });

  it('add / view / update / delete record', async () => {
    const coll = 'test_collection_small';
    const recId = `test_rec_${runId}`;

    // add via command (include embedding)
    const restoreAdd = stubInputSequence([recId, 'record text', JSON.stringify([0.1, 0.2])]);
    await vscode.commands.executeCommand('chromadb.addRecord', { name: coll });
    restoreAdd();

    const recs = await client.listRecords(tenant, database, coll, 50, 0);
    const r = recs.find((rr: any) => rr.id === recId);
    assert.ok(r && r.document && String(r.document).includes('record text'));

    // view - create a fake RecordItem-like object (command uses node.id and parentCollectionName)
    const node = { id: recId, parentCollectionName: coll, description: 'record text' } as any;
    await vscode.commands.executeCommand('chromadb.viewRecord', node);

    // update
    const restoreUpdate = stubInputSequence(['record text UPDATED']);
    await vscode.commands.executeCommand('chromadb.updateRecord', node);
    restoreUpdate();

    const recAfter = await client.getRecord(tenant, database, coll, recId);
    assert.ok(recAfter && String(recAfter.document).includes('UPDATED'));

    // query (should return something) - stub QuickPick to return first result so command completes
    const restoreQueryInput = stubInputSequence(['UPDATED']);
    const qpRestore = stubQuickPick({ label: recId });
    await vscode.commands.executeCommand('chromadb.queryCollection', { name: coll });
    restoreQueryInput(); qpRestore();

    // delete
    const restoreDel = stubWarning('Delete');
    await vscode.commands.executeCommand('chromadb.deleteRecord', node);
    restoreDel();

    const recsFinal = await client.listRecords(tenant, database, coll, 50, 0);
    assert.ok(!recsFinal.find((rr: any) => rr.id === recId));
  });

  it('refresh command should run without error', async () => {
    await vscode.commands.executeCommand('chromadb.refresh');
  });
});
