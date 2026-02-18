import { ChromaClient as SDKChromaClient, AdminClient } from 'chromadb';

export type ChromaRecord = { id: string; document?: string; metadata?: any; embedding?: number[] };

export class ChromaDbClient {
  private connected = false;
  private config: { host?: string; port?: number; ssl?: boolean; tenant?: string; database?: string; apiKey?: string } = {};
  private sdkClient?: any;
  private adminClient?: any;

  constructor() {
    // now backed by official `chromadb` SDK — no mock fallback
  }

  private sdkArgsFor(cfg?: Partial<{ host?: string; port?: number; ssl?: boolean; tenant?: string; database?: string; apiKey?: string }>) {
    const c = { ...this.config, ...(cfg || {}) };
    const headers = c.apiKey ? { 'x-chroma-token': c.apiKey } : undefined;
    return { host: c.host || 'localhost', port: c.port || 8000, ssl: !!c.ssl, tenant: c.tenant, database: c.database, headers } as any;
  }

  // low-level REST helper removed — use SDK AdminClient / Collection methods instead


  async connect(cfg: { host?: string; port?: number; ssl?: boolean; tenant?: string; database?: string; apiKey?: string }) {
    this.config = { ...(cfg || {}) };
    try {
      this.sdkClient = new SDKChromaClient(this.sdkArgsFor());
      this.adminClient = new AdminClient({ host: this.config.host || 'localhost', port: this.config.port || 8000, ssl: !!this.config.ssl, headers: this.config.apiKey ? { 'x-chroma-token': this.config.apiKey } : undefined });
      // quick health/version check
      await this.sdkClient.version();
      this.connected = true;
      return true;
    } catch (err) {
      this.connected = false;
      this.sdkClient = undefined;
      this.adminClient = undefined;
      return false;
    }
  }

  isConnected() {
    return this.connected;
  }

  async listTenants() {
    if (!this.connected) return [];
    // SDK does not expose a "listTenants" helper — return current tenant (most UIs use configured tenant)
    try {
      const identity = await this.sdkClient!.getUserIdentity();
      return [identity.tenant || (this.config.tenant || 'default_tenant')];
    } catch (err) {
      return [this.config.tenant || 'default_tenant'];
    }
  }

  async listDatabases(tenant?: string) {
    if (!this.connected || !this.adminClient) return [];
    const tName = tenant || this.config.tenant || 'default_tenant';
    try {
      const dbs = await this.adminClient.listDatabases({ tenant: tName });
      return Array.isArray(dbs) ? dbs.map((d: any) => d.name ?? d.id ?? String(d)) : [];
    } catch (err) {
      return [];
    }
  }

  async listCollections(tenant?: string, database?: string) {
    if (!this.connected) return [];
    try {
      const args = this.sdkArgsFor({ tenant, database });
      const client = new SDKChromaClient(args);
      const cols = await client.listCollections();
      return cols.map((c: any) => ({ id: c.id ?? c.name, name: c.name ?? c.id, count: (c.count ?? 0) }));
    } catch (err) {
      return [];
    }
  }

  private async resolveCollectionId(tenant: string | undefined, database: string | undefined, collectionOrId: string) {
    if (!this.connected) return collectionOrId;
    try {
      const args = this.sdkArgsFor({ tenant, database });
      const client = new SDKChromaClient(args);
      const cols = await client.listCollections();
      for (const item of cols) {
        if (item.id === collectionOrId || item.name === collectionOrId) return item.id ?? item.name;
      }
    } catch (err) {
      // fallback to original value
    }
    return collectionOrId;
  }

  async createCollection(tenant: string | undefined, database: string | undefined, name: string) {
    if (!this.connected) throw new Error('not connected');
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    try {
      await client.createCollection({ name });
      return true;
    } catch (err) {
      throw err;
    }
  }

  async deleteCollection(tenant: string | undefined, database: string | undefined, name: string) {
    if (!this.connected) throw new Error('not connected');
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    try {
      await client.deleteCollection({ name });
      return true;
    } catch (err) {
      throw err;
    }
  }

  async listRecords(tenant: string | undefined, database: string | undefined, collection: string, limit = 50, offset = 0) {
    if (!this.connected) return [];
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    try {
      const col = await client.getCollection({ name: collection });
      const res = await col.get({ limit, offset, include: ['documents', 'metadatas', 'embeddings'] });
      const ids = res.ids || [] as string[];
      const docs = res.documents || [] as Array<string | null>;
      const metas = res.metadatas || [] as Array<any>;
      const embs = res.embeddings || [] as Array<number[] | null>;
      const out: ChromaRecord[] = [];
      for (let i = 0; i < ids.length; i++) {
        out.push({ id: ids[i], document: (docs as any)[i] ?? undefined, metadata: (metas as any)[i] ?? undefined, embedding: (embs as any)[i] ?? undefined });
      }
      return out;
    } catch (err) {
      return [];
    }
  }

  async addRecord(tenant: string | undefined, database: string | undefined, collection: string, record: Partial<ChromaRecord>) {
    if (!this.connected) throw new Error('not connected');
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    const id = record.id || `id_${Date.now().toString(36)}${Math.random().toString(36).slice(2,8)}`;
    const embeddingToSend = (Array.isArray(record.embedding) && record.embedding.length > 0) ? record.embedding : [0.0];
    try {
      const col = await client.getOrCreateCollection({ name: collection });
      const addArgs: any = { ids: [id], documents: [record.document || ''], embeddings: [embeddingToSend] };
      if (record.metadata !== undefined && Object.keys(record.metadata || {}).length > 0) addArgs.metadatas = [record.metadata];
      await col.add(addArgs);
      return id;
    } catch (err) {
      throw err;
    }
  }

  async getRecord(tenant: string | undefined, database: string | undefined, collection: string, id: string) {
    if (!this.connected) return undefined;
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    try {
      const col = await client.getCollection({ name: collection });
      const res = await col.get({ ids: [id], include: ['documents', 'metadatas', 'embeddings'] });
      const ids = res.ids || [];
      if (!ids.length) return undefined;
      return { id: ids[0], document: (res.documents || [])[0], metadata: (res.metadatas || [])[0], embedding: (res.embeddings || [])[0] } as ChromaRecord;
    } catch (err) {
      return undefined;
    }
  }

  async queryCollection(tenant: string | undefined, database: string | undefined, collection: string, opts: { queryTexts?: string[]; queryEmbeddings?: number[][]; nResults?: number }) {
    if (!this.connected) return [];
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    try {
      const col = await client.getCollection({ name: collection });
      const qArgs: any = {};
      if (opts.queryTexts) qArgs.queryTexts = opts.queryTexts;
      if (opts.queryEmbeddings) qArgs.queryEmbeddings = opts.queryEmbeddings;
      qArgs.nResults = opts.nResults ?? 5;
      const res: any = await col.query(qArgs as any);
      return res as any; // keep shape similar to earlier REST response (ids/documents/metadatas/...)
    } catch (err) {
      return [];
    }
  }

  // tenant / database management (use REST for exact behavior)
  async createTenant(name: string) {
    if (!this.connected || !this.adminClient) throw new Error('not connected');
    try {
      await this.adminClient.createTenant({ name });
      return true;
    } catch (err) {
      throw err;
    }
  }

  async createDatabase(tenant: string, name: string) {
    if (!this.connected || !this.adminClient) throw new Error('not connected');
    try {
      await this.adminClient.createDatabase({ tenant, name });
      return true;
    } catch (err) {
      throw err;
    }
  }

  async deleteDatabase(tenant: string, name: string) {
    if (!this.connected || !this.adminClient) throw new Error('not connected');
    try {
      await this.adminClient.deleteDatabase({ tenant, name });
      return true;
    } catch (err) {
      throw err;
    }
  }

  async renameCollection(tenant: string | undefined, database: string | undefined, oldName: string, newName: string) {
    if (!this.connected) throw new Error('not connected');
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    try {
      const col = await client.getCollection({ name: oldName });
      await col.modify({ name: newName });
      return true;
    } catch (err) { throw err; }
  }

  async updateRecord(tenant: string | undefined, database: string | undefined, collection: string, record: Partial<ChromaRecord>) {
    if (!this.connected) throw new Error('not connected');
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    try {
      const col = await client.getCollection({ name: collection });
      const args: any = { ids: [record.id] };

      // documents/metadata update (only include embeddings when available or preserved)
      if (record.document !== undefined) args.documents = [record.document];
      if (record.metadata !== undefined) args.metadatas = [record.metadata];

      // prefer an explicit embedding; otherwise preserve the existing embedding for this id
      if (Array.isArray(record.embedding) && record.embedding.length > 0) {
        args.embeddings = [record.embedding];
      } else if (record.id) {
        // try to read the existing record and reuse its embedding so we don't trigger
        // the SDK to compute embeddings (which fails when no embedding-function is present)
        try {
          const existing = await this.getRecord(tenant, database, collection, record.id as string);
          if (existing?.embedding && Array.isArray(existing.embedding) && existing.embedding.length > 0) {
            args.embeddings = [existing.embedding];
          }
        } catch (_) {
          // ignore — if we can't fetch existing embedding, don't send embeddings and
          // let the server decide (may fail)
        }
      }

      await col.update(args);
      return true;
    } catch (err) { throw err; }
  }

  async deleteRecord(tenant: string | undefined, database: string | undefined, collection: string, id: string) {
    if (!this.connected) throw new Error('not connected');
    const client = new SDKChromaClient(this.sdkArgsFor({ tenant, database }));
    try {
      const col = await client.getCollection({ name: collection });
      await col.delete({ ids: [id] });
      return true;
    } catch (err) { throw err; }
  }
}

