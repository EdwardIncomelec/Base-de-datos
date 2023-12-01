const fs = require('fs');
const firebird = require('node-firebird');
const moment = require('moment');

// Configuración para evitar la zona horaria en el formateo de fechas y horas
moment.suppressDeprecationWarnings = true;
moment.createFromInputFallback = function (config) {
  config._d = new Date(config._i);
};

// Detalles de conexión Firebird
const firebirdOptions = {
  host: '127.0.0.1',
  port: 3050,
  database: 'C:/winpark/datos/eqparking.gdb',
  user: 'SYSDBA',
  password: 'masterkey'
};

// Función principal que extrae datos de todas las tablas y guarda en archivos CSV
async function extractDataToCsv(outputFolderPath) {
  // Realizar la conexión a Firebird
  firebird.attach(firebirdOptions, async (err, db) => {
    if (err) {
      console.error(`Error al conectar a la base de datos: ${err.message}`);
      return;
    }

    try {
      // Obtener la lista de tablas en la base de datos
      const result = await queryPromise(db, `
        SELECT RDB$RELATION_NAME AS TABLE_NAME
        FROM RDB$RELATIONS
        WHERE RDB$SYSTEM_FLAG = 0
        ORDER BY TABLE_NAME
      `);

      // Procesar cada tabla y guardar en un archivo CSV por separado
      for (const table of result) {
        const tableName = table.TABLE_NAME;
        if (!tableName.toLowerCase().startsWith('datos') && !tableName.toLowerCase().startsWith('hopec')) {
          await processTable(db, tableName, `${outputFolderPath}/${tableName}.csv`);
        } else {
          console.log(`La tabla ${tableName} se omitirá.`);
        }
      }

      console.log('Extracción de datos completada. Archivos CSV guardados en:', outputFolderPath);
    } catch (error) {
      console.error(`Error durante la extracción de datos: ${error.message}`);
    } finally {
      // Cerrar la conexión después de procesar todas las tablas
      db.detach();
    }
  });
}

async function processTable(db, tableName, outputPath) {
  return new Promise(async (resolve, reject) => {
    console.log(`Procesando tabla: ${tableName}`);

    try {
      // Ejecutar una consulta SQL para cada tabla
      const result = await queryPromise(db, `SELECT * FROM ${tableName}`);

      if (result.length > 0) {
        // Guardar los resultados en un archivo CSV
        const stream = fs.createWriteStream(outputPath, { flags: 'w' });

        // Escribir encabezados
        const headers = Object.keys(result[0]).join(',');
        stream.write(`${headers}\n`);

        // Escribir cada fila en el archivo con formateo de fechas y reemplazo de valores vacíos
        result.forEach(row => {
          // Verificar y rellenar columnas vacías con ceros
          Object.keys(row).forEach(key => {
            if (row[key] === null || row[key] === undefined) {
              row[key] = 0;
            }
          });

          // Marcar las columnas de tipo BLOB con "BLOB_DATA"
          Object.keys(row).forEach(key => {
            if (Buffer.isBuffer(row[key])) {
              row[key] = 'BLOB_DATA';
            }
          });

          // Formatear fechas y horas sin zona horaria
          const formattedRow = Object.keys(row).map(key => {
            if (row[key] instanceof Date) {
              return moment(row[key]).format('YYYY-MM-DD HH:mm:ss');
            }
            return row[key];
          }).join(',');

          stream.write(`${formattedRow}\n`);
        });

        // Cerrar el stream
        stream.end();

        console.log(`Procesamiento completado para la tabla: ${tableName}. Archivo CSV guardado en: ${outputPath}`);
      } else {
        console.warn(`No hay datos para procesar. La tabla ${tableName} está vacía.`);
      }

      resolve();
    } catch (error) {
      console.error(`Error al procesar la tabla ${tableName}: ${error.message}`);
      reject(error);
    }
  });
}

function queryPromise(db, sql) {
  return new Promise((resolve, reject) => {
    db.query(sql, (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}

// Llamada a la función principal para extraer datos de todas las tablas y guardar en CSV
extractDataToCsv('c:/Proyectos/informes/prueba/');