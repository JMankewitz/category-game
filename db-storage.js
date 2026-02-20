/**
 * Database storage helper for Cloud Storage persistence
 * Syncs SQLite database file to/from Google Cloud Storage
 */

const fs = require('fs').promises;
const path = require('path');

let storage = null;
let bucket = null;

// Initialize Cloud Storage if credentials are available
function initStorage() {
    if (process.env.GOOGLE_CLOUD_PROJECT || process.env.GCS_BUCKET) {
        try {
            // Try to require @google-cloud/storage (may not be available in local dev)
            const { Storage } = require('@google-cloud/storage');
            storage = new Storage();
            const bucketName = process.env.GCS_BUCKET || `${process.env.GOOGLE_CLOUD_PROJECT}-category-game-db`;
            bucket = storage.bucket(bucketName);
            console.log(`Cloud Storage initialized with bucket: ${bucketName}`);
            return true;
        } catch (error) {
            // Silently fail if @google-cloud/storage is not available (local development)
            if (error.code === 'MODULE_NOT_FOUND') {
                console.log('Cloud Storage module not found, using local storage only (local development mode)');
            } else {
                console.warn('Cloud Storage initialization failed, using local storage only:', error.message);
            }
            return false;
        }
    }
    return false;
}

/**
 * Download database from Cloud Storage if it exists
 */
async function downloadDatabase(localPath) {
    if (!bucket) return false;
    
    try {
        const fileName = 'game_data.db';
        const file = bucket.file(fileName);
        
        const [exists] = await file.exists();
        if (!exists) {
            console.log('No existing database found in Cloud Storage');
            return false;
        }
        
        await file.download({ destination: localPath });
        console.log(`Database downloaded from Cloud Storage to ${localPath}`);
        return true;
    } catch (error) {
        console.warn('Failed to download database from Cloud Storage:', error.message);
        return false;
    }
}

/**
 * Upload database to Cloud Storage
 */
async function uploadDatabase(localPath) {
    if (!bucket) return false;
    
    try {
        const fileName = 'game_data.db';
        const file = bucket.file(fileName);
        
        await file.save(await fs.readFile(localPath), {
            metadata: {
                contentType: 'application/x-sqlite3',
            },
        });
        
        console.log(`Database uploaded to Cloud Storage`);
        return true;
    } catch (error) {
        console.error('Failed to upload database to Cloud Storage:', error.message);
        return false;
    }
}

/**
 * Setup periodic database sync to Cloud Storage
 */
function setupPeriodicSync(localPath, intervalMs = 5 * 60 * 1000) {
    if (!bucket) return null;
    
    const interval = setInterval(async () => {
        try {
            await uploadDatabase(localPath);
        } catch (error) {
            console.error('Periodic database sync failed:', error.message);
        }
    }, intervalMs);
    
    console.log(`Periodic database sync enabled (every ${intervalMs / 1000}s)`);
    return interval;
}

/**
 * Setup graceful shutdown handler to sync database
 */
function setupShutdownSync(localPath) {
    if (!bucket) return;
    
    const syncAndExit = async (signal) => {
        console.log(`Received ${signal}, syncing database before shutdown...`);
        try {
            await uploadDatabase(localPath);
            console.log('Database synced successfully');
        } catch (error) {
            console.error('Failed to sync database on shutdown:', error.message);
        }
        process.exit(0);
    };
    
    process.on('SIGTERM', () => syncAndExit('SIGTERM'));
    process.on('SIGINT', () => syncAndExit('SIGINT'));
}

module.exports = {
    initStorage,
    downloadDatabase,
    uploadDatabase,
    setupPeriodicSync,
    setupShutdownSync
};

