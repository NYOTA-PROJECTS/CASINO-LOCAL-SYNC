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

const tablesToUpdate= [
    "Shops",
    "Caisses",
    "Settings",
    "Users",
    "UserCashbacks",
    "Cashbacks",
    "UserReferrals",
    "SettingSponsorings",
];

function compareData(data1, data2) {
    // Logique de comparaison de la base de données
    // Comparaison des ID
    const newData = [];
    for (const entry of data1) {
        if (!data2.some((item) => item.id === entry.id)) {
            newData.push(entry);
        }
    }
    return newData;
}

// Fonction principale pour comparer et synchroniser les données
async function compareAndSyncData() {
    try {
        // Vérifier la connexion Internet avant de continuer
        const internetConnectionAvailable = await checkInternetConnection();
        if (!internetConnectionAvailable) {
            console.log('La connexion Internet n\'est pas disponible. Impossible de synchroniser les données.');
            writeLogMessage(`Connexion Internet indisponible. - ${new Date().toLocaleString()}`);
            return;
        }

        // Logique de comparaison et de synchronisation
        // Connexion aux bases de données
        const db1Client = new Client(db1Config);
        const db2Client = new Client(db2Config);

        await db1Client.connect();
        await db2Client.connect();

        let syncPerformed = false; // Variable pour suivre si des synchronisations ont été effectuées

        // Pour chaque table à synchroniser
        for (const tableName of tablesToSync) {
            // Récupération des données de la table de la base de données 1
            const db1Data = await getData(db1Client, tableName);
            // Récupération des données de la table de la base de données 2
            const db2Data = await getData(db2Client, tableName);

            // Comparaison des données pour détecter les nouvelles informations
            const newDataFromDb1 = compareData(db1Data, db2Data);
            const newDataFromDb2 = compareData(db2Data, db1Data);

            // Synchronisation des nouvelles informations dans les deux sens
            if (await syncData(newDataFromDb1, db2Client, tableName)) {
                syncPerformed = true; // Marquer que des synchronisations ont été effectuées
            }
            if (await syncData(newDataFromDb2, db1Client, tableName)) {
                syncPerformed = true; // Marquer que des synchronisations ont été effectuées
            }

            const logMessage = `Synchronisation de la table ${tableName} terminée - ${new Date().toLocaleString()}\n`;
            writeLogMessage(logMessage);
            console.log(`Synchronisation de la table ${tableName} terminée.`);
        }

        // Fermeture des connexions aux bases de données
        await db1Client.end();
        await db2Client.end();

        if (syncPerformed) {
            console.log("Comparaison et synchronisation terminées avec succès.");
        } else {
            console.log("Aucune synchronisation nécessaire.");
        }
    } catch (error) {
        console.error("Une erreur est survenue :", error);
        const errorMessage = `Une erreur est survenue : ${error.message} - ${new Date().toLocaleString()}\n`;
        writeLogMessage(errorMessage);
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

// Exécution de la fonction principale toutes les minutes
setInterval(compareAndSyncData, 10000);