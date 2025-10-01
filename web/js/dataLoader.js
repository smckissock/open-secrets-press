import * as duckdb from '@duckdb/duckdb-wasm';

let dbInstance = null;

async function initDuckDB() {
    if (dbInstance) return dbInstance;
    
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
    const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], {type: 'text/javascript'})
    );
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);
    
    dbInstance = db;
    return db;
}

export async function loadParquetData(relativePath) {
    const db = await initDuckDB();
    const conn = await db.connect();
    
    // Get absolute URL
    const baseUrl = window.location.origin + window.location.pathname.replace(/\/[^\/]*$/, '');
    const parquetUrl = `${baseUrl}/${relativePath}`;
    
    // Register the parquet file
    const filename = relativePath.split('/').pop();
    await db.registerFileURL(
        filename,
        parquetUrl,
        duckdb.DuckDBDataProtocol.HTTP,
        false
    );
    
    // Query the parquet file
    const result = await conn.query(`SELECT * FROM '${filename}'`);
    
    // Convert to array of objects
    const data = result.toArray().map(row => Object.fromEntries(row));
    
    await conn.close();
    
    return data;
}

export async function terminateDuckDB() {
    if (dbInstance) {
        await dbInstance.terminate();
        dbInstance = null;
    }
}