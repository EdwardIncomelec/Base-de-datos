///Este script se encarga de tartar la información almacenada en la base de datos, convirtiendo las tablas
///que contienen dicha información organizada en columnas y descargandolas (Solo las tablas que contienen datos) 
///en archivos .txt que luego son tomados para implementar esa información en un libro de excel

const fs = require('fs');
const xlsx = require('xlsx');
const csv = require('csv-parse');
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

// Función principal que combina ambas funcionalidades
async function processFirebirdDataAndImportToExcel(folderPath) {
  const workbook = xlsx.utils.book_new();
  const usedSheetNames = new Set();

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

      // Procesar cada tabla y esperar a que se complete antes de pasar a la siguiente
      for (const table of result) {
        await processTable(db, table.TABLE_NAME);
      }

      // Cerrar la conexión después de procesar todas las tablas
      db.detach();
      console.log('Procesamiento de todas las tablas completado');

      // Importar datos a Excel desde archivos CSV y TXT
      importDataToFolder(folderPath, workbook, usedSheetNames);

    } catch (error) {
      console.error(`Error durante el procesamiento: ${error.message}`);
      db.detach(); // Asegurar que la conexión se cierre en caso de error
    }
  });
}

function importDataToFolder(folderPath, workbook, usedSheetNames) {
  const filenames = fs.readdirSync(folderPath);

  for (const filename of filenames) {
    if (filename.endsWith('.csv') || filename.endsWith('.txt')) {
      const data = readDataFile(`${folderPath}/${filename}`);

      if (data.length > 0) {
        let sheetName = getValidSheetName(filename);

        // Asegurar que el nombre de la hoja sea único
        while (usedSheetNames.has(sheetName)) {
          sheetName += '_'; // Añadir un guion bajo para hacerlo único
        }
        usedSheetNames.add(sheetName);

        const organizedData = organizeDataForExcel(data);
        const sheet = xlsx.utils.aoa_to_sheet(organizedData);
        xlsx.utils.book_append_sheet(workbook, sheet, sheetName);
      } else {
        console.warn(`El archivo ${filename} está vacío y se omitió.`);
      }
    }
  }

  if (workbook.SheetNames.length > 0) {
    writeWorkbookToFile('c:/Python1/prueba/tablas.xlsx', workbook);
  } else {
    console.warn('No se crearon hojas en el libro de Excel porque todos los archivos estaban vacíos o no eran archivos CSV o TXT.');
  }
}

function processTable(db, tableName) {
  return new Promise((resolve, reject) => {
    console.log(`Procesando tabla: ${tableName}`);

    // Ejecutar una consulta SQL para cada tabla
    db.query(`SELECT * FROM ${tableName}`, (err, result) => {
      if (err) {
        console.error(`Error al consultar la tabla ${tableName}: ${err.message}`);
        reject(err);
        return;
      }

      console.log(`Procesando resultados de la tabla: ${tableName}`);

      if (result.length > 0) {
        // Guardar los resultados en un archivo de texto
        const filePath = `c:/Python1/prueba/${tableName}.txt`;
        const stream = fs.createWriteStream(filePath, { flags: 'w' });

        // Escribir encabezados
        const headers = Object.keys(result[0]).join('\t');
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
          }).join('\t');

          stream.write(`${formattedRow}\n`);
        });

        // Cerrar el stream
        stream.end();

        console.log(`Procesamiento completado para la tabla: ${tableName}`);
        resolve();
      } else {
        console.warn(`No hay datos para procesar. Está vacía la tabla ${tableName}.`);
        resolve();
      }
    });
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

function organizeDataForExcel(data) {
    if (data.length === 0) {
      return [];
    }
  
    const hasHeaders = Array.isArray(data[0]) && data[0].every(cell => typeof cell === 'string');
    const headers = hasHeaders ? data[0] : Array.from({ length: data[0].length }, (_, index) => `Column${index + 1}`);
    const startIndex = hasHeaders ? 1 : 0;
  
    const organizedData = [headers, ...data.slice(startIndex)];
  
    return organizedData;
  }
  

function writeWorkbookToFile(filename, workbook) {
  try {
    xlsx.writeFile(workbook, filename);
    console.log('Libro de Excel creado exitosamente.');
  } catch (err) {
    console.error('Error al guardar el libro de Excel:', err);
  }
}

function readDataFile(filePath) {
  try {
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    if (filePath.endsWith('.csv')) {
      return csvToList(fileContent);
    } else if (filePath.endsWith('.txt')) {
      return fileContent.split('\n').map(line => [line.trim()]);
    }
  } catch (error) {
    console.error(`Error al leer el archivo ${filePath}:`, error);
    return [];
  }
}

function csvToList(csvString) {
  const options = {
    columns: true,
    skip_empty_lines: true,
  };

  const records = csv.parseSync(csvString, options);

  const data = records.map(record => Object.values(record));

  return data;
}

function getValidSheetName(sheetName) {
  if (sheetName.length > 20) {
    return sheetName.slice(0, 20);
  }
  return sheetName;
}

processFirebirdDataAndImportToExcel('c:/Python1/prueba/');
