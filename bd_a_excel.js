const fs = require('fs').promises;
const firebird = require('node-firebird');
const moment = require('moment');
const Papa = require('papaparse');
const XLSX = require('xlsx');
const readline = require('readline').createInterface({
  input: process.stdin,
  output: process.stdout
});
const program = require('commander');

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

// Función principal que extrae datos de Firebird y procesa el directorio CSV
async function main() {
  const outputFolderPath = await askUserForOutputPath();

  // Crear un nuevo libro de trabajo Excel
  const wb = XLSX.utils.book_new();

  // Extraer datos de Firebird
  await extractDataToCsv(outputFolderPath, wb);

  // Procesar directorio CSV y generar el archivo Excel
  await procesarDirectorio(outputFolderPath, wb);

  // Escribir el libro de trabajo Excel en un archivo
  XLSX.writeFile(wb, `${outputFolderPath}/salida.xlsx`);
  console.log('Proceso completado.');
}

function askUserForOutputPath() {
  return new Promise((resolve) => {
    readline.question('Ingrese la ruta donde desea guardar los archivos CSV y el archivo Excel: ', (path) => {
      readline.close();
      resolve(path);
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

// Función principal que extrae datos de todas las tablas y guarda en archivos CSV
async function extractDataToCsv(outputFolderPath, wb) {
  return new Promise((resolve, reject) => {
    // Realizar la conexión a Firebird
    firebird.attach(firebirdOptions, async (err, db) => {
      if (err) {
        console.error(`Error al conectar a la base de datos: ${err.message}`);
        reject(err);
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
        resolve();
      } catch (error) {
        console.error(`Error durante la extracción de datos: ${error.message}`);
        reject(error);
      } finally {
        // Cerrar la conexión después de procesar todas las tablas
        db.detach();
      }
    });
  });
}

async function processTable(db, tableName, outputPath) {
  return new Promise(async (resolve, reject) => {
    console.log(`Procesando tabla: ${tableName}`);

    try {
      // Ejecutar una consulta SQL para cada tabla
      const result = await queryPromise(db, `SELECT * FROM ${tableName}`);

      if (result.length > 0) {
        // Importar fs dentro de la función
        const fs = require('fs');

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

// Función para procesar el directorio CSV y generar el archivo Excel
async function procesarDirectorio(outputFolderPath, wb) {
  try {
    // Obtener la lista de archivos en el directorio
    const archivosCSV = await fs.readdir(outputFolderPath);

    // Función para procesar cada archivo CSV
    const procesarArchivo = async (archivo) => {
      // Omitir archivos que comienzan por "regis"
      if (archivo.toLowerCase().startsWith('regis')) {
        console.log(`Archivo omitido: ${archivo}`);
        return;
      }

      // Construir la ruta completa al archivo CSV
      const rutaCSV = `${outputFolderPath}/${archivo}`;

      // Leer el contenido del archivo CSV
      const csvData = await fs.readFile(rutaCSV, 'utf8');

      // Utilizar PapaParse para analizar el contenido CSV
      const result = await new Promise((resolve) => {
        Papa.parse(csvData, {
          header: true,
          complete: (result) => resolve(result),
        });
      });

      // Generar un nombre único para la hoja de trabajo en caso de duplicados
      let sheetName = limpiarNombreHoja(archivo.replace('.csv', ''));
      let sheetIndex = 1;

      while (wb.Sheets[sheetName]) {
        sheetIndex++;
        sheetName = limpiarNombreHoja(archivo.replace('.csv', '')) + '_' + sheetIndex;
      }

      // Convertir los datos analizados a una hoja de trabajo Excel
      const ws = XLSX.utils.json_to_sheet(result.data.map(rellenarCamposVacios), {
        header: Object.keys(result.data[0]),
      });

      // Agregar la hoja de trabajo al libro de trabajo Excel
      XLSX.utils.book_append_sheet(wb, ws, sheetName);
    };

    // Crear un array de promesas para el procesamiento paralelo de archivos
    const promesasProcesamiento = archivosCSV.map(procesarArchivo);

    // Esperar a que todas las promesas se resuelvan
    await Promise.all(promesasProcesamiento);

    // Escribir el libro de trabajo Excel en un archivo
    XLSX.writeFile(wb, `${outputFolderPath}/salida.xlsx`);
    console.log('Proceso completado.');
  } catch (error) {
    // Manejar errores durante el procesamiento
    console.error('Error durante el procesamiento:', error);
  }
}

// Función para limpiar el nombre de la hoja y asegurar que no exceda los 31 caracteres
function limpiarNombreHoja(nombre) {
  return nombre.substring(0, 31).trim();
}

// Función para rellenar campos vacíos en un objeto de datos con ceros
function rellenarCamposVacios(obj) {
  const newObj = { ...obj }; // Crear un nuevo objeto para no modificar el original
  Object.keys(newObj).forEach((key) => {
    if (newObj[key] === null || newObj[key] === undefined || newObj[key] === '') {
      newObj[key] = 0;
    }
  });
  return newObj;
}

// Iniciar el script
main();