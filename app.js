const { Client } = require('pg');
const fs = require("fs");
const ping = require('ping');

// Fonction pour vérifier la connexion Internet en pingant une adresse IP
async function checkInternetConnection() {
    try {
        const res = await ping.promise.probe('1.1.1.1');
        return res.alive; // Retourne true si la connexion est disponible, sinon false
    } catch (error) {
        console.error('Erreur lors de la vérification de la connexion Internet :', error);
        writeLogMessage(`Erreur lors de la vérification de la connexion Internet : ${error}`);
        return false; // En cas d'erreur, considérez que la connexion n'est pas disponible
    }
}

// Configuration des connexions aux bases de données
const db1Config = {
    host: "194.163.180.27",
    user: "postgres",
    password: "Admin@Casino2024",
    database: "database_casino_test",
    dialect: 'postgres',
    port: 5432,
    logging: true
};

const db2Config = {
    host: "127.0.0.1",
    user: "root",
    password: "root",
    database: "database_casino_local",
    dialect: 'postgres',
    port: 5432,
    logging: true
};

// Fonction pour récupérer les données des tables
async function getData(client, tableName) {
    try {
        const result = await client.query(`SELECT * FROM "${tableName}"`);
        return result.rows;
    } catch (error) {
        throw error;
    }
}

// Fonction pour synchroniser les nouvelles informations
async function syncData(newData, client, tableName) {
    try {
        // Vérifier si newData est défini
        if (!newData) {
            console.log(`Aucune nouvelle information à synchroniser de la table "${tableName}".`);
            const logMessage = `Aucune nouvelle information à synchroniser de la table "${tableName}" - ${new Date().toLocaleString()}\n`;
            writeLogMessage(logMessage);
            return false; // Indique qu'il n'y a pas eu de synchronisations pour cette table
        }

        // Vérifier si newData contient des données
        if (newData.length > 0) {
            // Obtenez les noms des colonnes pour la table spécifique
            const columns = Object.keys(newData[0]);

            // Démarrer une transaction
            await client.query('BEGIN');

            for (const entry of newData) {
                // Construisez la partie de la requête SQL avec les noms des colonnes
                const columnsString = columns.map(col => `"${col}"`).join(', ');

                // Construisez les placeholders pour les valeurs des colonnes
                const placeholders = columns.map((_, idx) => `$${idx + 1}`).join(', ');

                // Construisez la requête d'insertion avec les noms des colonnes et les placeholders
                const query = `INSERT INTO "${tableName}" (${columnsString}) VALUES (${placeholders})`;

                // Ajoutez les valeurs de l'entrée courante au tableau values
                const values = Object.values(entry);

                // Exécutez la requête d'insertion avec les valeurs correspondantes
                await client.query(query, values);
            }

            // Valider la transaction
            await client.query('COMMIT');
            console.log(`Synchronisation des nouvelles informations de la table "${tableName}" terminée.`);
            return true; // Indique qu'il y a eu des synchronisations pour cette table
        } else {
            console.log(`Aucune nouvelle information à synchroniser de la table "${tableName}".`);
            const logMessage = `Aucune nouvelle information à synchroniser de la table "${tableName}" - ${new Date().toLocaleString()}\n`;
            writeLogMessage(logMessage);
            return false; // Indique qu'il n'y a pas eu de synchronisations pour cette table
        }
    } catch (error) {
        // En cas d'erreur, annuler la transaction
        await client.query('ROLLBACK');
        console.error(`Une erreur est survenue lors de la synchronisation des données de la table "${tableName}":`, error);
        throw error;
    }
}

// Fonction pour mettre à jour les données existantes
async function updateData(updatedData, client, tableName) {
    try {
        console.log(`Mise à jour des données de la table "${tableName}"...`);
        if (updatedData.length > 0) {
            await client.query('BEGIN');

            for (const entry of updatedData) {
                const setColumns = Object.keys(entry).map((key, idx) => `"${key}" = $${idx + 1}`).join(', ');

                const query = `UPDATE "${tableName}" SET ${setColumns} WHERE "id" = $${Object.keys(entry).length + 1} AND "updatedAt" < $${Object.keys(entry).length + 2}`;
                const values = [...Object.values(entry), entry.id, entry.updatedAt];
                await client.query(query, values);
            }

            await client.query('COMMIT');
            console.log(`Mise à jour des données de la table "${tableName}" terminée.`);
            return true;
        } else {
            console.log(`Aucune donnée à mettre à jour pour la table "${tableName}".`);
            const logMessage = `Aucune donnée à mettre à jour pour la table "${tableName}" - ${new Date().toLocaleString()}\n`;
            writeLogMessage(logMessage);
            return false;
        }
    } catch (error) {
        await client.query('ROLLBACK');
        console.error(`Une erreur est survenue lors de la mise à jour des données de la table "${tableName}":`, error);
        throw error;
    }
}

// Liste des noms de table dans l'ordre spécifié
const tablesToSync = [
    "Shops",
    "Caisses",
    "Settings",
    "Users",
    "UserCashbacks",
    "Cashbacks",
    "UserReferrals",
    "Transactions",
    "SettingSponsorings",
];

const tablesToUpdate = [
    "Shops",
    "Caisses",
    "Settings",
    "Users",
    "UserCashbacks",
    "Cashbacks",
    "UserReferrals",
    "SettingSponsorings",
];

function compareDataForInsert(data1, data2) {
    const newData = [];

    for (const entry of data1) {
        const existingEntry = data2.find(item => item.id === entry.id);
        if (!existingEntry) {
            newData.push(entry);
        }
    }

    return { newData };
}

function compareDataForUpdate(data1, data2) {
    const updatedData = [];

    for (const entry of data1) {
        const matchingEntry = data2.find(item => item.id === entry.id);

        if (matchingEntry && entry.updatedAt !== matchingEntry.updatedAt) {
            updatedData.push(entry);
        }
    }

    return { updatedData };
}

// Fonction principale pour comparer et synchroniser les données entre deux bases de données
async function compareAndSyncData() {
    // Créer des instances de client pour les deux bases de données
    const clientDB1 = new Client(db1Config);
    const clientDB2 = new Client(db2Config);

    try {
        // Se connecter aux deux bases de données
        await clientDB1.connect();
        await clientDB2.connect();

        // Vérifier la connexion Internet avant de procéder
        const isConnected = await checkInternetConnection();
        if (!isConnected) {
            console.log("La connexion Internet n'est pas disponible. Veuillez vérifier votre connexion et réessayer plus tard.");
            return;
        }

        // Boucle à travers les tables à synchroniser
        for (const tableName of tablesToSync) {
            console.log(`Comparaison et synchronisation des données pour la table "${tableName}"...`);

            // Récupérer les données des deux bases de données
            const dataDB1 = await getData(clientDB1, tableName);
            const dataDB2 = await getData(clientDB2, tableName);

            // Comparer les données pour les insertions et les mises à jour
            const { newData } = compareDataForInsert(dataDB1, dataDB2);
            const { updatedData } = compareDataForUpdate(dataDB1, dataDB2);

            // Synchroniser les nouvelles données
            await syncData(newData, clientDB2, tableName);

            // Mettre à jour les données existantes
            await updateData(updatedData, clientDB2, tableName);
        }

        console.log("Comparaison et synchronisation des données terminées avec succès.");
    } catch (error) {
        console.error("Une erreur est survenue lors de la comparaison et de la synchronisation des données :", error);
    } finally {
        // Fermer les connexions client après utilisation
        await clientDB1.end();
        await clientDB2.end();
    }
}

function writeLogMessage(logMessage) {
    const logFileName = 'synchronization.log';
    const maxLines = 1000; // Nombre maximal de lignes à conserver dans le fichier de log

    // Lire le contenu actuel du fichier de log
    let existingLog = '';
    if (fs.existsSync(logFileName)) {
        existingLog = fs.readFileSync(logFileName, 'utf8');
    }

    // Séparer les lignes du fichier de log existant
    let lines = existingLog.trim().split('\n');

    // Supprimer les lignes excédentaires si nécessaire
    if (lines.length >= maxLines) {
        lines = lines.slice(-maxLines);
    }

    // Ajouter le nouveau message de journal à la fin
    lines.push(logMessage);

    // Écrire le contenu mis à jour dans le fichier de log
    fs.writeFileSync(logFileName, lines.join('\n'));
}

// Appeler la fonction principale pour comparer et synchroniser les données
compareAndSyncData();
